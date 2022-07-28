"""
Webscraper that pulls Race/Race Results from HorseRacingNation.com
This creates Pandas Dataframes for each Race/Race Results based on the 3 Data tables for each Race

Tables:
    Race (race information header)
    Race Results
    Runner
    Pool
"""

import warnings
import pandas as pd
from utils import log_debug, fetch_session, log_error, log_info, log_warn, set_log_level
from lxml.etree import tostring
from lxml.html import fromstring
warnings.simplefilter(action='ignore', category=FutureWarning)

_session = fetch_session()


def splice_column(record, element=0):
    try:
        return record.split("  ")[element]
    except:
        return record


def trasnform_dataframe(record):
        # record['ML'] = "("+str(record['ML']).replace("nan","") + ")"
        record['Horse'] = record['Horse / Sire'].apply(splice_column, element=0)
        record['Sire'] = record['Horse / Sire'].apply(splice_column, element=1)
        record['Trainer'] = record['Trainer / Jockey'].apply(splice_column, element=0)
        record['Jockey'] = record['Trainer / Jockey'].apply(splice_column, element=1)



def horse_racing_scrape(days=['all'], debug=False):
    global _session
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"
    }

    main_df = None
    table_dfs = {}
    log_debug(f'fetching Main Page: {days} : {len(days)}')
    main_res = _session.get("https://entries.horseracingnation.com/entries-results", headers = headers)
    main_html = fromstring(main_res.content)

    log_debug('Extracting Nav Links')
    nav_links = main_html.xpath("//li[@class='nav-item']//a[contains(@class,'nav-link')]/@href")

    log_info(f'Found Nav Links: {len(nav_links)}')
    
    if days[0] != 'all':
        nav_links = [f'/entries-results/{x}' for x in days]
        log_debug(f'Filtering results: {days}')

    for link in nav_links:
        print(link)
        datekey = link[link.rfind('/') + 1:]
        table_dfs[datekey] = {}

        track_links = None
        nav_res = _session.get("https://entries.horseracingnation.com"+link, headers=headers)
        nav_html = fromstring(nav_res.content)

        try:
            track_links = nav_html.xpath("//h2[contains(text(),'Tracks')]//following-sibling::div/table//td/a/@href")
            if not track_links:
                log_warn('No Race Data on this page, skipping link')
                continue

        except Exception as e:
            log_warn(f'Failed to load link: {link}')
            continue

        main_df = pd.DataFrame()
        log_debug(f'Found {len(track_links)} Track links (Races) for {link} (Date)')
        
        for t_link in track_links:
            log_debug(f'Scraping Page: https://entries.horseracingnation.com{t_link}')
            res = _session.get("https://entries.horseracingnation.com"+t_link, headers=headers)
            html = fromstring(res.content)
            data = res.text

            try:
                log_debug('Parsing Scratched names from Table')
                scratched = html.xpath("//tr[@class='scratched']/td[contains(@data-label,'Program')]")
                for scratch in scratched:
                    to_rem = tostring(scratch).decode("utf-8")
                    data = data.replace(to_rem, '<td>TRUE</td>')
            except Exception as e:
                log_warn(f'Failed to read scratched names on page: {t_link}')

            html = fromstring(data)
            all_races = html.xpath("//div[@class='my-5']")

            print(f"    {t_link} Races ({len(all_races)})")

            for race in all_races:
                tables = pd.read_html(tostring(race))
                results = tables[0][['PP', 'Horse / Sire', 'Trainer / Jockey','ML', '#']].copy()

                trasnform_dataframe(results)
                results = results[['PP', 'Horse','Sire', 'Trainer','Jockey','ML',"#"]].fillna('')
                try:
                    runner_table = tables[1]
                    for i in range(len(runner_table)):
                        check = runner_table.loc[i,'Runner'].strip()
                        for j in range(len(results)):
                            if check == results.loc[j,'Horse']:
                                runner_table.loc[i,'Horse Number'] = results.loc[j,'PP']
                                break
                    runner_table = runner_table[['Runner', 'Horse Number','Win', 'Place','Show']].fillna('')
                except:
                    runner_table = None
                ap_dic = {}
                race_title = race.xpath("*//a[contains(@class,'race-')]/text()")[0].replace("\n","").strip()
                ap_dic['Race Track'] = race_title.split("Race")[0].strip()
                ap_dic['Race Number'] = race_title.split("Race")[1].replace(",","").replace("#","").strip()
                ap_dic['Race Time'] = race.xpath("*//a[contains(@class,'race-')]//time/text()")[0].replace("\n","").strip()
                race_dis = race.xpath("//div[contains(@class,'race-distance')]/text()")[0].strip()
                ap_dic['Length'] = race_dis.split(",")[0].strip()
                ap_dic['Surface'] = race_dis.split(",")[1].strip()
                ap_dic['Race Class'] = race_dis.split(",", 2)[2].strip()
                restrictions = race.xpath("//div[contains(@class,'race-restrictions')]/text()")[0].strip()
                ap_dic['Sex'] = restrictions.split("|")[0].strip()
                ap_dic['Age'] = restrictions.split("|")[1].strip()
                ap_dic['Purse'] = race.xpath("//div[contains(@class,'race-purse')]/text()")[0].replace("Purse:","").strip()
                bet_type_df = pd.DataFrame(columns=['Bet Types','#'])
                bet_types = race.xpath(".//p[@class='race-wager-text']/text()")[0]
                for idx,type in enumerate(bet_types.split("/")):
                    if idx == 0:
                        ap_dic['Bet Types'] = type.strip()
                        ap_dic['#'] = idx+1
                    else:
                        bet_type_df.at[idx-1,'Bet Types'] = type.strip()
                        bet_type_df.at[idx-1,'#'] = idx+1
                try:
                    also_rans = race.xpath(".//div[contains(@class,'also-rans')]/text()")[0].replace("Also rans:","").strip()
                    also_rans_df = pd.DataFrame(columns=['Also Rans'])
                    for id,ran in enumerate(also_rans.split(",")):
                        also_rans_df.loc[id,'Also Rans'] = ran.strip()
                except:
                    also_rans_df = None
                try:
                    pool_df = tables[2]
                    for i in range(len(pool_df)):
                        pool_df.at[i,'Finish'] = "( "+str(pool_df.at[i,'Finish']).replace("nan","")+ ")"
                    race_fraction = race.xpath(".//div[contains(@class,'race-fractions')]/text()")[0].replace("Fractions and final time: :","").strip()
                    pool_df.at[0,'Fraction time'] = race_fraction
                except:
                    pool_df = None

                if ap_dic['Race Track'] not in table_dfs[datekey]:
                    table_dfs[datekey][ap_dic['Race Track']] = {'id' : '00'} # Initialize with temp id
                
                table_dfs[datekey][ap_dic['Race Track']][ap_dic['Race Number']] = {
                    "ap" : ap_dic, 
                    "bet_type" : bet_type_df, 
                    "race_results" : results, 
                    "runners" : runner_table, 
                    "also_ran" : also_rans_df,
                    "pool" : pool_df
                }

                df = pd.DataFrame()
                df = df.append(ap_dic, ignore_index=True)
                df = df.append(bet_type_df, ignore_index=True)
                df = pd.concat([df,results],axis=1, ignore_index=True)
                df = pd.concat([df,runner_table],axis=1, ignore_index=True)
                df = pd.concat([df,also_rans_df],axis=1, ignore_index=True)
                df = pd.concat([df,pool_df],axis=1, ignore_index=True)
                main_df = main_df.append(df, ignore_index=True)

                # # Grab SampleSet for Testing
                # if debug:
                #     if len(table_dfs[datekey].keys()) >= 3:
                #         return table_dfs

        log_debug('Trimming Data Columns')
        try:
            main_df.columns = ['Race Track','Race Number','Race Time','Length','Surface','Race Class','Sex','Age','Purse','Bet Types','#','PP','Horse','Sire','Trainer','Jockey','Morning Line ML','Scratched','Runner Name','Horse Number','Win','Place','Show','Runner Name','Pool','Finish',	'$2 Payout','Total Pool','Fractions and Final time']
        except:
            log_warn('Failed inital Column Parse, attempting 2nd Parse')
            try:
                main_df.columns = ['Race Track','Race Number','Race Time','Length','Surface','Race Class','Sex','Age','Purse','Bet Types','#','PP','Horse','Sire','Trainer','Jockey','Morning Line ML','Scratched']
            except:
                log_error('Failed 2nd attempt Column Parse, Data is lost')
                return main_df

        main_df.fillna("",inplace=True)
        main_df.to_csv(link.split("/")[-1]+"-V3.csv",index=False)

    return table_dfs

if __name__ == "__main__":
    set_log_level("INFO")
    log_info('Starting Web Scrapper')
    obbb = horse_racing_scrape(['2022-07-18'])
    print(obbb.keys())