import time
from scipy import io
import pandapower as pp
import random
import pandas as pd
import numpy as np
import os


class DataPre:

    def __init__(self, data_dir):
        self.data_dir = data_dir

    def raw_pre(self):
        # raw_bus
        bus_cols = ['bus number', 'bus type, PQ bus = 1 / PV bus = 2 / reference bus = 3 / isolated bus = 4',
                     'Pd, real power demand (MW)', 'Qd, reactive power demand (MVAr)', 'Gs, shunt conductance (MW demanded at V = 1.0 p.u.)', 'Bs, shunt susceptance (MVAr injected at V = 1.0 p.u.)',
                     'area number, (positive integer)', 'Vm, voltage magnitude (p.u.)', 'Va, voltage angle (degrees)', 'baseKV, base voltage (kV)', 'zone, loss zone (positive integer)',
                     'maxVm, maximum voltage magnitude (p.u.)', 'minVm, minimum voltage magnitude (p.u.)']
        raw_bus = pd.read_excel(f'{self.data_dir}/raw data/bus.xls', names=bus_cols, header=None, index_col=None)

        # raw_branch
        branch_cols = ['f, from bus number', 't, to bus number', 'r, resistance (p.u.)', 'x, reactance (p.u.)', 'b, total line charging susceptance (p.u.)', 'rateA, MVA rating A (long term rating)',
                     'rateB, MVA rating B (short term rating)', 'rateC, MVA rating C (emergency rating)', 'ratio, transformer off nominal turns ratio ( = 0 for lines )',
                     'angle, transformer phase shift angle (degrees), positive => delay', 'initial branch status, 1 - in service, 0 - out of service',
                     'minimum angle difference, angle(Vf) - angle(Vt) (degrees)', 'maximum angle difference, angle(Vf) - angle(Vt) (degrees)']
        raw_branch = pd.read_excel(f'{self.data_dir}/raw data/branch.xls', names=branch_cols, header=None, index_col=None)

        #raw_gen
        gen_cols = ['bus number', 'Pg, real power output (MW)', 'Qg, reactive power output (MVAr)', 'Qmax, maximum reactive power output (MVAr)', 'Qmin, minimum reactive power output (MVAr)',
                     'Vg, voltage magnitude setpoint (p.u.)', 'mBase, total MVA base of this machine, defaults to baseMVA', 'status,  >  0 - machine in service  <= 0 - machine out of service',
                     'Pmax, maximum real power output (MW)', 'Pmin, minimum real power output (MW)', 'Pc1, lower real power output of PQ capability curve (MW)',
                     'Pc2, upper real power output of PQ capability curve (MW)', 'Qc1min, minimum reactive power output at Pc1 (MVAr)', 'Qc1max, maximum reactive power output at Pc1 (MVAr)',
                     'Qc2min, minimum reactive power output at Pc2 (MVAr)', 'Qc2max, maximum reactive power output at Pc2 (MVAr)', 'ramp rate for load following/AGC (MW/min)',
                     'ramp rate for 10 minute reserves (MW)', 'ramp rate for 30 minute reserves (MW)', 'ramp rate for reactive power (2 sec timescale) (MVAr/min)',
                     'APF, area participation factor']
        raw_gen = pd.read_excel(f'{self.data_dir}/raw data/gen.xls', names=gen_cols, header=None, index_col=None)

        #raw_bus_name
        bus_name_cols = ['bus name']
        raw_bus_name = pd.read_excel(f'{self.data_dir}/raw data/bus_name.xls', names=bus_name_cols, header=None, index_col=None)

        # raw_gen_name
        gen_name_cols = ['bus name']
        raw_gen_name = pd.read_excel(f'{self.data_dir}/raw data/gen_name.xls', names=gen_name_cols, header=None, index_col=None)

        # bus
        bus = pd.concat([raw_bus_name, raw_bus], axis=1)
        # gen
        gen = pd.concat([raw_gen_name, raw_gen], axis=1)
        # branch
        branch = raw_branch
        # bus number 재정렬 (1020 -> 1)
        bus_new_num = np.arange(1, len(bus) + 1)
        for i in range(len(bus)):
            old = bus['bus number'][i]
            new = bus_new_num[i]
            bus['bus number'] = bus['bus number'].replace(old, new)
            gen['bus number'] = gen['bus number'].replace(old, new)
            branch['f, from bus number'] = branch['f, from bus number'].replace(old, new)
            branch['t, to bus number'] = branch['t, to bus number'].replace(old, new)
        
        # 엑셀 저장
        DataPre.write_data(self, bus, folder_name='pre data', file_name='bus')
        DataPre.write_data(self, gen, folder_name='pre data', file_name='gen')
        DataPre.write_data(self, branch, folder_name='pre data', file_name='branch')

        return bus, gen, branch

    def load_data(self, folder_name, file_name):
        # file_name = 'bus' / 'bus_name' / 'gen' / 'gen_name' / 'branch' / 'red_bus' / 'red_gen' / 'red_branch'
        xls_data = pd.read_excel(f'{self.data_dir}/{folder_name}/{file_name}.xlsx')

        return xls_data

    def create_folder(self, directory):
        directory = directory
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Creating directory. ' + directory)

    def write_data(self, df, folder_name, file_name):
        DataPre.create_folder(self, f'{self.data_dir}/{folder_name}')
        df = df.reset_index(drop=True)
        df.to_excel(f'{self.data_dir}/{folder_name}/{file_name}.xlsx', index=False)
        print(f'*---{file_name}.xlsx 파일 저장완료---*')


if __name__ == '__main__':
    print('**---PSS/E 데이터 전처리---**')
    start = time.time()
    data_dir = './운영_DB_평일_11H_72000_최대'
    data_pre = DataPre(data_dir)
    data_pre.raw_pre()
    end = time.time()
    print(f'\nIt takes {(end - start)/60} min.')