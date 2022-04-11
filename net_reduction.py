import data_pre
import time
from scipy import io
import pandapower as pp
import random
import pandas as pd
import numpy as np
import os


class NetRed:

    def __init__(self):
        pass

    def net_reduciton(self, bus, gen, branch, save):
        # 지역단위로 Bus 구성
        # phase 1: 부하에서 열소비계수가 0인 재생에너지의 출력을 고려
        print('->>>phase 1: 부하에서 재생에너지 출력 고려...')
        for i in gen.index:
            if (gen['Quadratic price'][i] == 0) and (gen['Linear price'][i] == 0) and (gen['No load price'][i] == 0):
                bus_num = gen['bus number'][i]
                gen_P = gen['Pmax, maximum real power output (MW)'][i]
                gen_Q = gen['Qmax, maximum reactive power output (MVAr)'][i]
                load_idx = bus.index[bus['bus number'] == bus_num].tolist()
                if len(load_idx) > 0:
                    load_idx = load_idx[0]
                    bus['Pd, real power demand (MW)'][load_idx] = bus['Pd, real power demand (MW)'][load_idx] - gen_P
                    bus['Qd, reactive power demand (MVAr)'][load_idx] = bus['Qd, reactive power demand (MVAr)'][
                                                                            load_idx] - gen_Q
                gen = gen.drop(i)

        # phase 2: 지역 내에 기준 모선 1개만 남기기
        print('->>>phase 2: 지역단위로 모선 구성...')
        # 슬랙 모선 정보
        slack_bus_idx = bus.index[bus['bus type, PQ bus = 1 / PV bus = 2 / reference bus = 3 / isolated bus = 4'] == 3].tolist()[0]
        slack_bus_area = bus['region'][slack_bus_idx]
        # 축약 시작
        total_bus_num = len(bus)
        total_branch_num = len(branch)
        region_list = list(set(list(bus['region'])))
        for region_name in region_list:
            selected_idx = []
            for bus_num in bus['bus number']:
                bus_idx = bus.index[bus['bus number'] == bus_num].tolist()[0]
                if region_name == bus['region'][bus_idx]:
                    selected_idx.append(bus_idx)
            for i in range(len(selected_idx)):
                if i == 0:
                    pass
                else:
                    std_idx = selected_idx[0]
                    rem_idx = selected_idx[i]
                    std_bus = bus['bus number'][std_idx]
                    rem_bus = bus['bus number'][rem_idx]
                    bus['Pd, real power demand (MW)'][std_idx] = bus['Pd, real power demand (MW)'][std_idx] + \
                                                                 bus['Pd, real power demand (MW)'][rem_idx]
                    bus['Qd, reactive power demand (MVAr)'][std_idx] = bus['Qd, reactive power demand (MVAr)'][
                                                                           std_idx] + \
                                                                       bus['Qd, reactive power demand (MVAr)'][rem_idx]
                    bus = bus.drop(rem_idx)
                    gen['bus number'] = gen['bus number'].replace(rem_bus, std_bus)
                    branch['f, from bus number'] = branch['f, from bus number'].replace(rem_bus, std_bus)
                    branch['t, to bus number'] = branch['t, to bus number'].replace(rem_bus, std_bus)

        # 슬랙 모선 정보 표시
        slack_bus_area_idx = bus.index[bus['region'] == slack_bus_area].tolist()[0]
        bus['bus type, PQ bus = 1 / PV bus = 2 / reference bus = 3 / isolated bus = 4'][slack_bus_area_idx] = 3

        print(f'*---전체{total_bus_num}개 모선에서 {len(bus)}개로 축약완료---*')
        print(f'현재 모선 구성: {region_list}')

        # phase 3: 치환과정에서 생긴 동일 버스로 연결된 선로들을 제거
        print('->>>phase 3: From, To가 동일한 선로들을 제거...')
        for branch_idx in branch.index:
            from_bus_buff = branch['f, from bus number'][branch_idx]
            to_bus_buff = branch['t, to bus number'][branch_idx]
            if from_bus_buff == to_bus_buff:
                branch = branch.drop(branch_idx)
            elif from_bus_buff > to_bus_buff:
                branch['f, from bus number'][branch_idx] = to_bus_buff
                branch['t, to bus number'][branch_idx] = from_bus_buff

        print(f'*---전체{total_branch_num}개 선로에서 {len(branch)}개로 축약완료---*')

        # phase 4: 지역간 연결된 선로들을 하나의 선로로 축약(병렬 연결)
        print('->>>phase 4: 지역간 연결된 선로들을 하나의 선로로 축약(병렬 연결)...')
        total_branch_num = len(branch)
        for from_bus_num in bus['bus number']:
            for to_bus_num in bus['bus number']:
                branch_idx = branch.index[(branch['f, from bus number'] == from_bus_num) &
                                       (branch['t, to bus number'] == to_bus_num)].tolist()
                if len(branch_idx) > 1:
                    r_, x_, b_, rateA, rateB, rateC = [], [], [], [], [], []
                    for i in range(len(branch_idx)):
                        if i == 0:
                            # r, b에 0 값이 존재하는데, 0은 분모에 들어갈 수 없으므로, 예외처리를 해줘야 함
                            if branch['r, resistance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['x, reactance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['r, resistance (p.u.)'][branch_idx[i]] == 0 and\
                                    branch['x, reactance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(1 / branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['r, resistance (p.u.)'][branch_idx[i]] == 0 and \
                                 branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['x, reactance (p.u.)'][branch_idx[i]] == 0 and \
                                 branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['r, resistance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(1 / branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['x, reactance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch[1 / 'b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            else:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(1 / branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                        else:
                            # r, b에 0 값이 존재하는데, 0은 분모에 들어갈 수 없으므로, 예외처리를 해줘야 함
                            if branch['r, resistance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['x, reactance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['r, resistance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['x, reactance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(1 / branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['r, resistance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['x, reactance (p.u.)'][branch_idx[i]] == 0 and \
                                    branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['r, resistance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(1 / branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['x, reactance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch[1 / 'b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            elif branch['b, total line charging susceptance (p.u.)'][branch_idx[i]] == 0:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            else:
                                r_.append(1 / branch['r, resistance (p.u.)'][branch_idx[i]])
                                x_.append(1 / branch['x, reactance (p.u.)'][branch_idx[i]])
                                b_.append(1 / branch['b, total line charging susceptance (p.u.)'][branch_idx[i]])
                                rateA.append(branch['rateA, MVA rating A (long term rating)'][branch_idx[i]])
                                rateB.append(branch['rateB, MVA rating B (short term rating)'][branch_idx[i]])
                                rateC.append(branch['rateC, MVA rating C (emergency rating)'][branch_idx[i]])
                            branch = branch.drop(branch_idx[i])

                    # r, b에 0 값이 존재하는데, 0은 분모에 들어갈 수 없으므로, 예외처리를 해줘야 함
                    if sum(r_) == 0 and sum(x_) == 0 and sum(b_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 0
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 0
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 0
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    elif sum(r_) == 0 and sum(x_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 0
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 0
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 1 / sum(b_)
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    elif sum(r_) == 0 and sum(b_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 0
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 1 / sum(x_)
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 0
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    elif sum(x_) == 0 and sum(b_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 1 / sum(r_)
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 0
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 0
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    elif sum(r_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 0
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 1 / sum(x_)
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 1 / sum(b_)
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    elif sum(x_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 1 / sum(r_)
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 0
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 1 / sum(b_)
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    elif sum(b_) == 0:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 1 / sum(r_)
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 1 / sum(x_)
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 0
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)
                    else:
                        branch['r, resistance (p.u.)'][branch_idx[0]] = 1/sum(r_)
                        branch['x, reactance (p.u.)'][branch_idx[0]] = 1/sum(x_)
                        branch['b, total line charging susceptance (p.u.)'][branch_idx[0]] = 1/sum(b_)
                        branch['rateA, MVA rating A (long term rating)'][branch_idx[0]] = sum(rateA)
                        branch['rateB, MVA rating B (short term rating)'][branch_idx[0]] = sum(rateB)
                        branch['rateC, MVA rating C (emergency rating)'][branch_idx[0]] = sum(rateC)

                else:
                    pass

        print(f'*---전체{total_branch_num}개 선로에서 {len(branch)}개로 축약완료---*')

        # 현재 선로 연결 상태
        branch_connected = []
        for i in branch.index:
            from_bus_buff = branch['f, from bus number'][i]
            to_bus_buff = branch['t, to bus number'][i]
            from_bus_name = bus['region'][bus.index[bus['bus number'] == from_bus_buff].tolist()[0]]
            to_bus_name = bus['region'][bus.index[bus['bus number'] == to_bus_buff].tolist()[0]]
            branch_connected.append(f'{from_bus_name}-{to_bus_name}')
        print(f'현재 선로 구성: {branch_connected}\n')

        if save == True:
            # Write the data
            data_pre.write_data(df=bus, folder_name='reduction results', file_name='red_bus')
            data_pre.write_data(df=gen, folder_name='reduction results', file_name='red_gen')
            data_pre.write_data(df=branch, folder_name='reduction results', file_name='red_branch')

        return bus, branch, gen


if __name__ == '__main__':
    print('**---계통 축약---**')
    start = time.time()
    data_dir = './운영_DB_평일_11H_72000_최대'
    load_folder_name = 'pre data'
    data_pre = data_pre.DataPre(data_dir)
    net_red = NetRed()
    # Load the data
    bus = data_pre.load_data(folder_name=load_folder_name, file_name='bus')
    gen = data_pre.load_data(folder_name=load_folder_name, file_name='gen')
    branch = data_pre.load_data(folder_name=load_folder_name, file_name='branch')
    # Network reduction
    red_bus, red_branch, red_gen= net_red.net_reduciton(bus=bus, branch=branch, gen=gen, save=True)
    end = time.time()
    print(f'\nIt takes {(end - start)/60} min.')