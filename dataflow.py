import argparse
import json
import logging
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineResult

class PickBan(typing.NamedTuple):
    is_pick: str
    hero_id: int
    teamname: str
    winner: bool

def parse_json(element):
    return json.loads(element)
  
class get_picks_bans(beam.DoFn):
    def process(self, element):
        picks_bans = element["picks_bans"]
        for elem in picks_bans:
            if elem["team"] == 0:
                teamname = element["radiant_name"]
            else:
                teamname = element["dire_name"]
            if (elem["team"] == 0 and element["radiant_win"] == True) or (elem["team"] == 1 and element["radiant_win"] == False):
                winner = True
            else:
                winner = False
            yield (PickBan(
                is_pick = elem["is_pick"],
                hero_id = int(elem["hero_id"]),
                teamname = teamname,
                winner = winner
            ))

class calc_winrate(beam.DoFn):
    def process(self, element):
        key = element[0]
        val = element[1]
        win_list = val["wins"]
        win_val = 0
        if len(win_list) != 0:
            win_val = win_list[0]

        pick_list = val["picks"]
        pick_val = 0
        if len(pick_list) != 0:
            pick_val = pick_list[0]

        winrate = 0
        if pick_val != 0:
            winrate = win_val/pick_val
        if "totals" in val:
            yield (key, {'totals': val["totals"], 'bans': val["bans"], 'picks': val["picks"], 'winrate': winrate})
        else:
            yield (key, {'bans': val["bans"], 'picks': val["picks"], 'winrate': winrate})

def run(argv=None, save_main_session=True) -> PipelineResult:
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-cs553-459018/matches/')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://dataflow-cs553-459018/results/')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline = beam.Pipeline(options=pipeline_options)

    # Read JSON files
    parsedjson = pipeline | 'Read lines' >> beam.io.ReadFromText(known_args.input+"*.json") | 'ParseJson' >> beam.Map(parse_json)
    picks_bans = parsedjson | 'GetPicksBans' >> beam.ParDo(get_picks_bans())
    totals = picks_bans | beam.Map(lambda elem: (elem.hero_id, 1)) | "HeroTotals" >> beam.CombinePerKey(sum)
    # Picks pipeline
    picks = picks_bans | 'GetPicks' >> beam.Filter(lambda elem: elem.is_pick == True)
    total_picks = picks | 'TotalPicks' >> beam.Map(lambda elem: (elem.hero_id, 1)) | "PickPerHero" >> beam.CombinePerKey(sum)
    team_picks = picks | 'GroupPickByTeam' >> beam.Map(lambda elem: ((elem.hero_id, elem.teamname), 1)) | "TeamPickPerHero" >> beam.CombinePerKey(sum)
    # Bans pipeline
    bans = picks_bans | 'GetBans' >> beam.Filter(lambda elem: elem.is_pick == False)
    total_bans = bans | 'TotalBans' >> beam.Map(lambda elem: (elem.hero_id, 1)) | "BanPerHero" >> beam.CombinePerKey(sum)
    team_bans = bans | 'GroupBanByTeam' >> beam.Map(lambda elem: ((elem.hero_id, elem.teamname), 1)) | "TeamBanPerHero" >> beam.CombinePerKey(sum)
    # Wins pipeline
    wins = picks_bans | 'GetWins' >> beam.Filter(lambda elem: elem.is_pick == True and elem.winner == True)
    total_wins = wins | 'TotalWins' >>  beam.Map(lambda elem: (elem.hero_id, 1)) | "WinPerHero" >> beam.CombinePerKey(sum)
    team_wins = wins | 'GroupWinByTeam' >> beam.Map(lambda elem: ((elem.hero_id, elem.teamname), 1)) | "TeamWinPerHero" >> beam.CombinePerKey(sum)
    # Cogroup totals
    hero_totals = ({ 'totals': totals, 'bans': total_bans, 'picks': total_picks, 'wins': total_wins }) | "MergeHeroTotals" >> beam.CoGroupByKey() | "CalcHeroTotalWinrate" >> beam.ParDo(calc_winrate())
    hero_totals | "WriteHeroTotals" >> beam.io.WriteToText(known_args.output+"all_stats")
    # Cogroup teams
    team_totals = ({ 'bans': team_bans, 'picks': team_picks, 'wins': team_wins }) | "MergeTeamTotals" >> beam.CoGroupByKey() | "CalcTeamHeroTotalWinrate" >> beam.ParDo(calc_winrate())
    team_totals | "WriteTeamTotals" >> beam.io.WriteToText(known_args.output+"team_stats")

    result = pipeline.run()
    result.wait_until_finish()
    return result

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()