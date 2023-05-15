import os
import pandas as pd
import subprocess
from datetime import datetime


batch_version = "t4"
batch_time = datetime(2023, 5, 15, 13, 0, 0)
dt_string = batch_time.strftime("%m_%d_%Y_%H_%M")
batch_version = batch_version + '--' + dt_string

# Run output directory 
sim_output_dir = f'../sim_outputs/{batch_version}/'

default_tnc_share = '0.015' # Used in both table (veh number table and plugs table)
upper_tnc_share = '0.1'
lower_tnc_share = '0.005'

df_all = pd.DataFrame()
scenario_dict = {'vtype': [], 
                 'tnc_share':[],
                 'hc_access': []}
record_num = 0


for scenario in os.listdir(sim_output_dir):
    scenario_path = os.path.join(sim_output_dir,scenario)

    if os.path.isdir(scenario_path):
    # Get scenario parameters from scenario name
        scenario_items = scenario.split('_')
        tnc_str = scenario_items[1]
        hc_str = scenario_items[3]
        vtype_str = f'{scenario_items[5]}_{scenario_items[6]}_{scenario_items[7]}'
        
        if tnc_str not in scenario_dict['tnc_share']:
            scenario_dict['tnc_share'].append(tnc_str)
        if hc_str not in scenario_dict['hc_access']:
            scenario_dict['hc_access'].append(hc_str)
        if vtype_str not in scenario_dict['vtype']:
            scenario_dict['vtype'].append(vtype_str)


for scenario in os.listdir(sim_output_dir):
    scenario_path = os.path.join(sim_output_dir,scenario)

    if os.path.isdir(scenario_path):
        # Get scenario parameters from scenario name
        scenario_items = scenario.split('_')
        tnc_str = scenario_items[1]
        hc_str = scenario_items[3]
        vtype_str = f'{scenario_items[5]}_{scenario_items[6]}_{scenario_items[7]}'
        
        # Save unique scenario parameters to a dictionary
        if tnc_str not in scenario_dict['tnc_share']:
            scenario_dict['tnc_share'].append(tnc_str)
        if hc_str not in scenario_dict['hc_access']:
            scenario_dict['hc_access'].append(hc_str)
        if vtype_str not in scenario_dict['vtype']:
            scenario_dict['vtype'].append(vtype_str)
        
        # Concat all cbsa level tables
        df = pd.read_csv(os.path.join(scenario_path,'population_results.csv'))
        df['vtype'] = vtype_str
        df['tnc_share'] = tnc_str
        df['hc_access'] = hc_str
        df_all = pd.concat([df_all, df])
        record_num += 1



dff = df_all[df_all['tnc_share']==default_tnc_share] 

post_output1_path = os.path.join(sim_output_dir,'cbsa_plugs_all_scenarios.csv')
dff.to_csv(post_output1_path)
print(f'Table 1 finished: Aggregated {record_num} scenarios to {len(dff)} records, saved at {post_output1_path}.')


veh_wide_df = df_all.pivot(index = ['vtype','hc_access','cbsa_id'], columns = ['tnc_share'], values = 'num_vehs').reset_index()
tnc_columns = veh_wide_df.columns[-3:]
new_columns_name = ['vehs_at_' + col for col in tnc_columns]
veh_wide_df.rename(columns = dict(zip(tnc_columns, new_columns_name)), inplace= True)

post_output2_path = os.path.join(sim_output_dir,'cbsa_num_vehs_at_tnc_shares.csv')
veh_wide_df.to_csv(post_output2_path)
print(f'Table 2 finished: Aggregated {record_num} scenarios to {len(veh_wide_df)} records, saved at {post_output2_path}.')