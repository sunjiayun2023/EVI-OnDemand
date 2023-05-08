import yaml
import os
import pandas as pd
import subprocess
from datetime import datetime

# # Load the YAML data from a file
# with open('../scenarios/bau_baseline.yaml', 'r') as file:
#     yamlß_data = file.read()

# # Convert the YAML data to a dictionary
# dict_data = yaml.safe_load(yaml_data)

# # Print the dictionary data
# print(dict_data)

def import_scenario_vars(scenario):
    with open(scenario, 'r') as stream:
        input_dict = yaml.safe_load(stream)

    return input_dict

def modify_scenario_vars(scenario):
    scenario['utilization_perc'] = 0.2

    return scenario

def write_scenario_trace(output_dir, scenario):
    with open(f'{output_dir}/{scenario}.yaml', 'w') as outfile:
        yaml.dump(global_inputs, outfile, default_flow_style=False)

    return

if __name__ == "__main__":
    # Naming a batch process version 
    batch_version = "t2"
    now = datetime.now()
    dt_string = now.strftime("%m_%d_%Y_%H_%M")
    batch_version = batch_version + '--' + dt_string
    print (f'Simulation starts at {dt_string}...')
    # I/O
    # Read the default senario.yaml file to modify
    default_scenario = "bau_baseline.yaml"

    # Read vehicle type specific data to generate scnarios
    df_vtype = pd.read_csv("../scenarios/BEV_vehicle_types_230301.csv")

    # Scenario output directory, create folder if not exists 
    scenario_dir = "../scenarios/{}".format(batch_version)
    if not os.path.exists(scenario_dir):
        os.makedirs(scenario_dir)
        print (f'Created directory: {scenario_dir}')
    else:
        print (f'Directory already exists: {scenario_dir}')

    # Run output directory 
    output_dir = f'../sim_outputs/{batch_version}/'

    # Read the senario.yaml file into a dictionaty
    global_inputs = import_scenario_vars(default_scenario)

    # Update the values of the inputs in the scenario.yaml file
    global_inputs['output_dir'] = output_dir
    global_inputs['utilization_perc'] = 0.2
    for tnc_share in [0.005, 0.015, 0.03, 0.05, 0.1]:
        global_inputs['tnc_share'] = tnc_share
        for hc in ['100pct','0pct']:
            global_inputs['hc_scenario'] = f'{hc}_hc_access'
            for vtype in df_vtype['vehicle_id'].tolist():
                wh_mi = df_vtype.loc[df_vtype['vehicle_id'] == vtype, 'watt_hour_per_mile'].item()
                battery_kwh = df_vtype.loc[df_vtype['vehicle_id'] == vtype, 'battery_capacity_kwh'].item()
                dc_max_kw = df_vtype.loc[df_vtype['vehicle_id'] == vtype, 'max_charge_dc_kw'].item()

                global_inputs['base_wh_mi'] = wh_mi
                global_inputs['dcfc_max_kw'] = dc_max_kw
                global_inputs['veh_kwh_dict'] = {battery_kwh : 100}
                print(f"Processed tnc_share: {tnc_share}, hc_scenario: {hc} for vehicle type: {vtype}.")

                scenario = f"tnc_{tnc_share}_hc_{hc}_vtype_{vtype}"
                global_inputs['scenario_name'] = scenario
                # Write the yaml file to the scenario folder
                write_scenario_trace(scenario_dir, scenario)


# RUN MODEL
national_plugs_all_scenarios = pd.DataFrame()
loop_count = 0
for filename in os.listdir(scenario_dir):
    loop_count += 1
    
    # Loop countrol for debug
    if loop_count > 3:
        continue 

    scenario_name = filename.split('.yaml')[0]
    if filename.endswith('.yaml'):
        sub_script_path = "../src/ondemand_fleetsim_plugs.py"
        args = ['python', sub_script_path, os.path.join(scenario_dir,filename)]
        print(f'\n\nProcessing the {loop_count} scenario...')
        subprocess.run(args)
        national_plugs = pd.read_csv(f'{output_dir}{scenario_name}/national_plugs.csv')
        national_plugs_all_scenarios = pd.concat([national_plugs_all_scenarios, national_plugs], ignore_index = True)
national_plugs_all_scenarios.to_csv(f'{output_dir}national_plugs_all_scenarios.csv', index=False)
end_time = datetime.now().strftime("%m_%d_%Y_%H_%M")
print(f'\n\nAll Simulations completed! {end_time}')
