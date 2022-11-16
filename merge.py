# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : Li Xiaoran
# @Time     : 16/09/2021 10:57
import os
import argparse
import numpy as np
from core.merge_function import get_dataframe, concat_data, calculate_similarity, write_file, get_original_data, resolve_irrelevant_header_tail, \
    resolve_irrelevant_tail, resolve_irrelevant_header_first, resolve_irrelevant_header_second,get_columns_value
from core.log import Logger

def file_combine(folder_path,end_path,log):
    all_folder_list=os.listdir(folder_path)
    log.logger.info(f'总共有{len(all_folder_list)}个银行流水')
    for folder in all_folder_list:
        file_path = os.path.join(folder_path, folder)
        all_file_list = [b for b in os.listdir(file_path) if "$" not in b]
        log.logger.info(f'{folder}总共有{len(all_file_list)}个文件')
        mdict = {}
        for single_file in all_file_list:
            data_frame_dict = get_dataframe(file_path, single_file)
            for sheet_name, single_data_frame in data_frame_dict.items():

                single_data_frame = single_data_frame.replace(r'^\s*$', np.nan, regex=True)
                single_data_frame = single_data_frame.dropna(axis=0, how='all').reset_index(drop=True)
                single_data_frame = single_data_frame.dropna(axis=1, how='all').reset_index(drop=True)
                single_data_frame_concat = concat_data(single_data_frame)
                single_data_frame.to_excel("10.xlsx")
                alist = calculate_similarity(single_data_frame_concat)
                single_data_frame.to_excel("11.xlsx")
                if len(alist) > 0:
                    if len(alist)==1 and [0] in alist:
                        #log.logger.info(f'{folder}:{single_file}表头表尾都没有无关信息')
                        mdict = get_original_data(single_data_frame, single_file, folder, log, mdict)
                    elif alist[0][-1] >= 1 and len(alist[-1]) > 1 and alist[-1][0] > 2 * len(single_data_frame_concat) / 3:
                        #log.logger.info(f'{folder}:{single_file}表头表尾都有无关信息')
                        single_data_frame = single_data_frame[0:len(single_data_frame) - len(alist[-1])]
                        mdict = resolve_irrelevant_header_tail(single_data_frame, alist, folder, single_file, log, mdict)
                    elif len(alist) >1 and [0] in alist and alist[-1][0] >2*len(single_data_frame_concat)/3:
                        #log.logger.info(f'{folder}:{single_file}表尾有无关信息')
                        mdict = resolve_irrelevant_tail(single_data_frame,alist,single_file,folder,log,mdict)
                    elif len(alist)==1 and [0] not in alist and len(single_data_frame_concat) >50 and alist[0][-1] < 1*len(single_data_frame_concat)/3:
                        #log.logger.info(f'{folder}:{single_file}表头有无关信息第一种情况')
                        mdict =resolve_irrelevant_header_first(single_data_frame, alist, folder, mdict, single_file, log)
                    elif len(alist) == 1 and [0] not in alist and len(single_data_frame_concat) < 50 and alist[0][-1] < 1 * len(single_data_frame_concat) / 2:
                        #log.logger.info(f'{folder}:{single_file}表头有无关信息第二种情况')
                        mdict =resolve_irrelevant_header_first(single_data_frame, alist, folder, mdict, single_file, log)
                    elif len(alist[0]) > 1 and alist[0][-1] == single_data_frame_concat.isnull().sum(axis=1).argmin():
                        #log.logger.info(f'{folder}:{single_file}表头有无关信息的第三种情况')
                        mdict =resolve_irrelevant_header_first(single_data_frame, alist, folder, mdict, single_file, log)
                    elif len(alist[0]) > 1 and alist[0][-2] == single_data_frame_concat.isnull().sum(axis=1).argmin():
                        #log.logger.info(f'{folder}:{single_file}表头有无关信息的第四种情况')
                        mdict =resolve_irrelevant_header_second(single_data_frame, folder, mdict, single_file, log)
                    elif len(alist) == 2 and alist[-1][-1] < 1 * len(single_data_frame_concat) / 3:
                        #log.logger.info(f'{single_file}表头有无关信息的第五种情况')
                        single_data_frame_first = single_data_frame[alist[-1][-1]:]
                        single_data_frame_second = single_data_frame[(alist[-1][-1])-1:]
                        mdict =get_columns_value(single_data_frame_first, single_data_frame_second, folder,mdict, single_file, log)
                    elif len(alist) == 2 and len(alist[-1]) == 1 and alist[-1][0] > 2 * len(single_data_frame_concat) / 3 and alist[0][-1] < 1 * len(single_data_frame_concat) / 2:
                        #log.logger.info(f'{single_file}表头有无关信息的第六种种情况')
                        single_data_frame_first = single_data_frame[alist[0][-2]:]
                        single_data_frame_second = single_data_frame[(alist[0][-2]) - 1:]
                        # single_data_frame_second = single_data_frame[(alist[0][-1])-1:]
                        mdict =get_columns_value(single_data_frame_first, single_data_frame_second, folder,mdict, single_file, log)
                    else:
                        #log.logger.info(f'{folder}:{single_file}不做任何处理')
                        mdict =get_original_data(single_data_frame, single_file, folder, log, mdict)
                else:
                    #log.logger.info(f'{folder}:{single_file}不做任何处理')
                    mdict =get_original_data(single_data_frame, single_file, folder, log, mdict)
        write_file(mdict, end_path, folder, log)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--source_path", type=str, help="base path")
    parser.add_argument("--out_path", type=str, help="out path")
    parse_args = parser.parse_args()
    if not os.path.exists(parse_args.out_path):
        os.makedirs(parse_args.out_path)
    log = Logger(os.path.join(parse_args.out_path, 'all.log'), level='info')
    file_combine(parse_args.source_path, parse_args.out_path, log)
