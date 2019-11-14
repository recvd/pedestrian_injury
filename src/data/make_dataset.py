# -*- coding: utf-8 -*-
import re
import time
import json
import click
import pandas as pd
import logging
from pathlib import Path


def nets_sum_categories(df, years):
    """ Calculates sum categories based on more granular categories"""
    for year in years:
        df['ALC_{}'.format(year)] = df[['BAR_{}'.format(year), 'LIQ_{}'.format(year)]].any(axis=1) \
            .astype(int)
        df['NGT_{}'.format(year)] = df[['BAR_{}'.format(year), 'EAT_{}'.format(year), 'ART_{}'.format(year)]] \
            .any(axis=1) \
            .astype(int)

    return df


def classify_nets(df, class_dict, years):
    """ Faster implementation to classify a df of businesses for all years"""
    for business_class in class_dict.keys():
        for year in years:
            # Primary SIC code match
            sic_bool = df['SIC{}'.format(year)].isin(class_dict[business_class]['sic_exclusive'])

            # 6 digit SIC code match
            sic_6_bool = df['SIC6'].isin(class_dict[business_class]['sic_6'])

            # SIC range match
            sic_range_bool = pd.Series(0, index=df.index)
            for sic_range in class_dict[business_class]['sic_range']:
                sic_range_bool += ((df['SIC{}'.format(year)].astype(int) >= sic_range[0]) &
                                   (df['SIC{}'.format(year)].astype(int) <= sic_range[1])).astype(int)
            sic_range_boolk = sic_range_bool.astype(bool)

            # SICS that fall within ranges but shouldn't be included
            # Returns True for a business that does NOT fall within the sic_not codes
            sic_not_bool = ~df['SIC{}'.format(year)].isin(class_dict[business_class]['sic_not'])

            # construct final boolean series
            final_bool = (sic_bool | sic_6_bool | sic_range_bool) & sic_not_bool
            df['{}_{}'.format(business_class, year)] = final_bool.astype(int)

            # Add sum categories
    df = nets_sum_categories(df, years)

    return df


def wrangle_nets(data_path, years, class_dict):
    """Provides codes to transform raw NETS data into needed format for the pedestrian injury analysis
    :argument year: year to perform the analysis for
    returns: None (writes to file)
    """

    # these paths should be moved outside the scope of this function
    address_path = data_path / 'raw' / 'NETS2014_AddressSpecial00to14_sample.txt'
    sic_path = data_path / 'raw' / 'NETS2014_SIC_sample.txt'

    # read columns from data files and determine which ones to use
    with open(address_path, 'r') as add_file, open(sic_path, 'r') as sic_file:
        address_all_cols = add_file.readline().strip().split('\t')
        sic_all_cols = sic_file.readline().strip().split('\t')

    address_bool = lambda x: any(re.match(col, x) for col in ['Address', 'City[1-10]+', 'State', 'ZIP'])
    year_bool = lambda x: any(re.search(year, x) for year in years)

    address_cols = [x for x in address_all_cols if year_bool(x) and address_bool(x)]
    sic_cols = ['SIC6'] + [x for x in sic_all_cols if year_bool(x)]

    # read all addresses and rop rows that didn't exist in these years
    df_address = pd.read_table(address_path,
                               usecols=['DunsNumber'] + address_cols,
                               index_col='DunsNumber',
                               encoding='Windows-1252',
                               error_bad_lines=False,
                               dtype='object'
                               ) \
        .dropna(how='all') \
        .apply(lambda x: x.str.strip(), axis=0)

    # read all SIC codes and drop rows that didn't exist in these years
    df_sic = pd.read_table(sic_path,
                           usecols=['DunsNumber'] + sic_cols,
                           index_col='DunsNumber',
                           encoding='Windows-1252',
                           error_bad_lines=False,
                           dtype="object") \
        .dropna(subset=[x for x in sic_cols if x != 'SIC6'])

    # Some have address but no SIC, only take rows with both
    df_test_inner = df_address.join(df_sic, how='inner')

    # Classify the data and write to CSV
    t1 = time.time()
    print('Beginning Classification of NETS Data:')
    df_classified = classify_nets(df_test_inner, class_dict, years)
    print('Time to classify: {} seconds'.format(round(time.time() - t1, 2)))

    # formatting wrangled data filepath based on which years were used
    if len(years) > 1:
        write_file = 'PI_NETS_{}-{}.csv'.format(years[0], years[-1])
    else:
        write_file = 'PI_NETS_{}.csv'.format(years[0])

    df_classified.to_csv(data_path / 'processed' / write_file)


def wrangle_FARS():
    pass

# @click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main(data_path, years, class_dict):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    logger.info('wrangling NETS data')
    wrangle_nets(data_path, years, class_dict)
    logger.info('NETS wrangling complete: Data available in data/processed')

    # logger.info('wrangling FARS data')
    # wrangle_FARS()
    # logger.info('FARS wrangling complete: Data available in data/processed')

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    with open('../../config/category_config.json', 'r') as f:
        class_dict = json.load(f)

    raw_path = Path.cwd().parent.parent / 'data'

    main(raw_path, ['13', '14'], class_dict)
