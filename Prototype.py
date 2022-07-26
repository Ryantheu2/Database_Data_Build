'''This is the prototype code for translating MG Hedge fields that follow if then logic. Before running the
this code, it is important to set up all input and output files that are connected to this process and update their file
locations within the code. The following input data needs to be linked into the code: the prototype data (the excel
sheet that contains the logic for the if/then statements), the raw data (the input policy extract data sheet), STAT data
(the STAT output data), the GAAP data (the GAAP output data), the Input Hedge Ratio data(utility table for all the
hedge ratio data, and the global parameters data (excel sheet with all of the global parameter data). The user must
also create a file for the output data to go into. Once the user has created this file, they have to update the save
file folder path in the main method at the bottom of this code. Once these steps are complete, the user can run this
code.'''


import pandas as pd
from datetime import datetime
from datedelta import datedelta
from openpyxl import load_workbook
import warnings
warnings.filterwarnings("ignore")

# sets the output display in the console wider so that the whole of the DataFrame is visible
pd.set_option('display.width', 400)
pd.set_option('display.max_columns', 100)


# function for importing the prototypr (logic tab) data as well as the raw (policy extract) data
def import_prototype_data():
    prototype = pd.read_excel('C:\\Users\\td753lx\\Desktop\\MG Hedge Future State Mapping Document v1.0.xlsx'
                              , sheet_name='Iso')
    raw_data = pd.read_excel('C:\\Users\\td753lx\\Desktop\\EV-15_Prototype_Test_Policies v0.2.xlsx'
                             , sheet_name='Policy Extract').fillna('NONE')
    # iso_test
    # Testing Table

    return prototype, raw_data


# function to import the GAAP ad STAT output data. This data is used to compare the logic output to
def import_trail_data(trail):

    stat_data = pd.read_excel('C:\\Users\\td753lx\\Desktop\\EV-15_Prototype_Test_Policies v0.2.xlsx'
                              , sheet_name=trail + ' STAT')
    gaap_data = pd.read_excel('C:\\Users\\td753lx\\Desktop\\EV-15_Prototype_Test_Policies v0.2.xlsx'
                              , sheet_name=trail + ' GAAP_SOP')
    return stat_data, gaap_data


# this function begins the loops that go over each individual trail and field.
def current_value_set(prototype, raw_data, save_path):
    current_trail = ''
    current_var = ''
    source_field_names = list(filter(lambda x: 'Source' in x, list(prototype)))
    # creates a filtered list of all of the source field column names
    source_field_names = list(filter(lambda x: 'Value' in x, list(source_field_names)))
    # creates a filtered list of all of the policy extract field names column names
    policy_extract_field_names = list(filter(lambda x: 'Policy' in x, list(prototype)))
    # loop that goes down each row of prototype logic data
    for i, x in enumerate(prototype.iterrows()):
        # checks to see what trail (EV15, TLA, etc) code is on
        if current_trail == x[1]['Trail']:
            continue
        else:
            # if new trail, sets the new trail as the current trail and creates a new dataframe of only that trail type
            current_trail = x[1]['Trail']
            check_dataframe = pd.DataFrame(index=raw_data['POLICY'])
            print('\n' + current_trail)
            cut_frame_trail = prototype[prototype['Trail'] == current_trail]
            stat_data, gaap_data = import_trail_data(cut_frame_trail['Trail'].reset_index(drop=True)[0])
            # loops through each Prophet variable to check if it'schanged and then isolate that fields logic into
            # its own dataframe
            for ii, xx in enumerate(cut_frame_trail.iterrows()):
                if current_var == xx[1]['Prophet Variable Name']:
                    continue
                else:
                    current_var = xx[1]['Prophet Variable Name']
                    print('\n' + current_var)
                    cut_frame_var = cut_frame_trail[cut_frame_trail['Prophet Variable Name'] == current_var]
                    # run logic identifier function to check what the field logic is and run it to find the target output
                    output_list = logic_identifier(raw_data, source_field_names, policy_extract_field_names
                                                       , cut_frame_var)
                    # puts field simulation output into a list and appends that list to a dataframe that will be used to
                    # output the result data for each field indexed by its policy number
                    check_dataframe[current_var] = output_list
            # function to write data to output file
            write(check_dataframe, current_trail, save_path)
            # function to compare simulated output data with real STAT and GAAP output data
            result_looper(check_dataframe, prototype, stat_data, gaap_data)


# reads and enacts prototype data logic
def logic_identifier(raw_data, source_fields, extract_fields, cut_frame):
    output_list = []
    # loops through isolated field dataframe
    for ii, w in enumerate(raw_data.iterrows()):
        use_cut_frame = cut_frame
        # loops through each source field (to check what operation or comparison needs to be done)
        for e, source in enumerate(source_fields):
            extract = extract_fields[e]
            # print('source', source)
            # print('extract', extract)
            use_cut_frame, iii = conditions(use_cut_frame, w, ii, raw_data, source, extract)
            # if statment checking if the field dataframe is equal to one. if it is, we've found our output answer
            if use_cut_frame.shape[0] == 1:
                use_cut_frame = use_cut_frame.reset_index(drop=True)
                answer = use_cut_frame['Prophet Target Value'][0]
                # if statement that checks if target value answer is a variable or special case answer that needs to be
                # converted through additional code or by selecting values from a table
                if type(answer) is str and ('-' in answer or 'input_global' in answer or 'v_' in answer):
                    if 'v_gmwb_reset' in answer:
                        answer = gmwb_reset(raw_data, w[1]['POLICY'])
                    elif 'v_age' in answer:
                        answer = age_2(raw_data, w[1]['POLICY'])
                    elif 'GMIB-fee-amt' in answer:
                        answer = gmib_lif_cntg_prem_rt(raw_data, w[1]['POLICY'])
                    elif 'input_global' in answer:
                        answer = global_params(answer)
                    else:
                        # if the value doesn't meet any of the special cases above then it goes through the
                        # transform_results function for general transform cases
                        answer = transform_result(use_cut_frame['Alias Variable'][0], w[1]['POLICY'], raw_data, answer)
                # prints out answer by policy (target value result, policy number, and the last condition data passed
                print(answer, w[1]['POLICY'], w[1][use_cut_frame[extract]][0])
                # appends answer to output list
                output_list.append(answer)
                break
            else:
                continue
    return output_list


# function to check each source field for what condition it is trying to apply within the logic (equal to, greater
# or less than, else statment, etc.)
def conditions(cut_frame, w, ii, raw_data, source_field_name, extract_field_name):
    # loop that goes over each individual condition in a source field column until a match is found
        for iii, y in enumerate(cut_frame[source_field_name]):
            cut_frame = cut_frame.reset_index(drop=True)
            name = cut_frame[extract_field_name][iii]
            # checks if it is a greater than condition
            if '>' in str(y):
                if '=' in str(y):
                    y = y.replace('>= ', '')
                    y = date_time_check(y)

                    if y <= raw_data[cut_frame[extract_field_name][iii]][ii]:
                        cut_frame = cut_frame[
                            cut_frame[source_field_name] == cut_frame[source_field_name][iii]]
                        return cut_frame, iii
                else:
                    y = y.replace('> ', '')
                    y = date_time_check(y)
                    if y < raw_data[cut_frame[extract_field_name][iii]][ii]:
                        cut_frame = cut_frame[
                            cut_frame[source_field_name] == cut_frame[source_field_name][iii]]
                        return cut_frame, iii
            # checks if it is a less than condition
            elif '<' in str(y):
                if '=' in str(y):
                    y = y.replace('<= ', '')
                    # checks if it is a date time that needs to be formatted correctly
                    y = date_time_check(y)

                    if y >= raw_data[cut_frame[extract_field_name][iii]][ii]:
                        # if source field value meets this condition, the field dataframe is cut for all rows that meet
                        # this specific condition
                        cut_frame = cut_frame[
                            cut_frame[source_field_name] == cut_frame[source_field_name][iii]]
                        return cut_frame, iii
                else:
                    y = y.replace('< ', '')
                    y = date_time_check(y)

                    if y > raw_data[cut_frame[extract_field_name][iii]][ii]:
                        cut_frame = cut_frame[
                            cut_frame[source_field_name] == cut_frame[source_field_name][iii]]
                        return cut_frame, iii
            # checks if it is a not equal to condition
            elif '!' in str(y):
                if y.replace('!= ', '') != raw_data[cut_frame[extract_field_name][iii]][ii]:
                    cut_frame = cut_frame[cut_frame[source_field_name] == cut_frame[source_field_name][iii]]
                    return cut_frame, iii
            # checks if it is a LIKE condition
            elif 'LIKE' in str(y):
                if y.replace('LIKE ', '') in raw_data[cut_frame[extract_field_name][iii]][ii]:
                    cut_frame = cut_frame[cut_frame[source_field_name] == cut_frame[source_field_name][iii]]
                    return cut_frame, iii
            else:
                # if all other checks before this fail then it checks if the extract and source field (actual) values
                # equal each other (this will happen if there is a condition checking for a specific trail like src
                # system = EV15 for example)
                if str(y).replace("'", '') == name:
                    cut_frame = cut_frame[cut_frame[source_field_name] == str(y)]
                    return cut_frame, iii
                # check if the source field value equals the assigned policy extract value
                elif str(y).replace("'", '') in str(w[1][name]):
                    cut_frame, check = equal_operation(w, iii, y, cut_frame, source_field_name, extract_field_name)
                    if check == 1:
                        return cut_frame, iii
                else:
                    # if all other checks fail this statement checks if it's an else statement
                    cut_frame, check = else_operation(y, iii, cut_frame, source_field_name)
                    if check == 1:
                        return cut_frame, iii


# function that checks if the source field and extract values meet the equal condition then filters the dataframe
#  on rows that meet said condition
def equal_operation(w, iii, y, cut_frame, source_field_name, extract_field_name):
    check = 0
    if str(w[1][cut_frame[extract_field_name][iii]]).replace(' ', '').replace("'", '') \
            == str(y).replace("'", '').replace(' ', ''):
        cut_frame = cut_frame[(cut_frame[source_field_name] == str(y))
                              & (cut_frame[extract_field_name] == cut_frame[extract_field_name][iii])]
        check = 1
    return cut_frame, check


# function that checks if the source field and extract values meet the else condition then filters the dataframe
#  on rows that meet said condition
def else_operation(y, iii, cut_frame, source_field_name):
    check = 0
    if 'ELSE' in str(y) and iii == cut_frame.shape[0] - 1:
        cut_frame = cut_frame[cut_frame[source_field_name] == 'ELSE'].reset_index(drop=True)
        check = 1
    return cut_frame, check


# formats any datetime values correctly
def date_time_check(y):
    if '-' in y:  # change later!
        y = datetime.strptime(y, '%d-%b-%Y')
    else:
        y = float(y)
    return y


# function that checks the target output value for general transformations it may need such as greatest functions
# and simple arithmetic
def transform_result(answer, policy, raw_data, pre_val):
    if 'GREATEST' in pre_val:
        data = answer.split(', ')
        answer_list = []
        for x in data:
            data = raw_data[raw_data['POLICY'] == policy].reset_index(drop=True)[x][0]
            answer_list.append(data)
        answer = max(answer_list)
        return answer
    data = raw_data[raw_data['POLICY'] == policy].reset_index(drop=True)[answer][0]

    if ' - ' in pre_val:
        data = data - float(pre_val.split(' - ')[1].replace(';', ''))
        # after each operation, checks if the variable needs to be saved as a float or an int
        if data % 1 == 0:
            data = int(data)
    elif ' + ' in pre_val:
        data = data + float(pre_val.split(' + ')[1].replace(';', ''))
        if data % 1 == 0:
            data = int(data)
    elif ' * ' in pre_val:
        data = data * float(pre_val.split(' * ')[1].replace(';', ''))
        if data % 1 == 0:
            data = int(data)
    elif ' / ' in pre_val:
        data = data / float(pre_val.split(' / ')[1].replace(';', ''))
        if data % 1 == 0:
            data = int(data)
    return data


# variable to determine gmwb_reset variable
def gmwb_reset(raw_data, policy):
    data = raw_data[raw_data['POLICY'] == policy].reset_index(drop=True)
    if data['GMWB_INDICATOR'][0] == 1 or data['GMWB_INDICATOR'][0] == 2:
        date = data['GMWB_LAST_RESET_DT'][0] + datedelta(months=data['GMWB_OPT_GUAR_RESET_WAIT_PER'][0] * 12)
        date = (date - data['VAL_DT']) / 365.25
        if date[0].days < 0:
            return 0
        else:
            hours = (date[0].seconds / 60) / 60
            if hours >= 12:
                day = date[0].days + 1
                return day
            else:
                day = date[0].days
                return day


# function to determine age_2 variable
def age_2(raw_data, policy):
    data = raw_data[raw_data['POLICY'] == policy].reset_index(drop=True)
    year = (data['VAL_DT'][0].year - data['DOB1'][0].year)
    age2 = int(year) - 3
    return age2


# function to determine gmib_lif_cntg_prem_rt variable
def gmib_lif_cntg_prem_rt(raw_data, policy):
    data = raw_data[raw_data['POLICY'] == policy].reset_index(drop=True)
    # reads hedge ratio file into a dataframe
    hedge_ratio = pd.read_excel('C:\\Users\\vn197nn\\Desktop\\Transform_Files\\INPUT_HEDGE_RATIO.xlsx'
                                , sheet_name='INPUT_HEDGE_RATIO')
    hedge_ratio = hedge_ratio[(hedge_ratio['EFF_YEAR'] == raw_data['VAL_DT'][0].year)
                              & hedge_ratio['EFF_MONTH'] == raw_data['VAL_DT'][0].month].reset_index(drop=True)
    if hedge_ratio.empty:
        hedge_ratio = 0
    else:
        hedge_ratio = hedge_ratio['GMIB3_HRATIO'][0]
    lif_cntg_prem_rt = data['GMIB_FEE_AMOUNT'][0] * hedge_ratio
    if lif_cntg_prem_rt % 1 == 0:
        lif_cntg_prem_rt = int(lif_cntg_prem_rt)
    return lif_cntg_prem_rt


# function to determine global_params variables
def global_params(parameter):
    parameter = parameter.replace(';', '').replace(' ', '').split('.')
    # reads global parameters file into a dataframe
    global_params_data = pd.read_excel('C:\\Users\\vn197nn\\Desktop\\Transform_Files\\INPUT_GLOBAL_PARAMETERS.xlsx'
                                       , sheet_name='INPUT_GLOBAL_PARAMETERS')
    convert_answer = global_params_data[parameter[1].upper()][0]
    return convert_answer


# function that loops over answer_list and checks which variables were simulated correctly and outputs which were not
def result_looper(check_dataframe, prototype, stat_data, gaap_data):
    amount_correct = 0
    overall_amount = check_dataframe.shape[1]
    column_names = list(check_dataframe)
    print('\nThe following don\'t match: ')
    for name in column_names:
        current_frame = prototype[prototype['Prophet Variable Name'] == name].reset_index(drop=True)
        trail = current_frame['Feed Type'][0].lower().replace(', ', '')
        # runs function to check which real output values (GAAP or STAT) to compare the simulated values to
        result = result_checker(current_frame, trail, stat_data, gaap_data, check_dataframe[name])
        if result == 1:
            amount_correct += 1
        else:
            print(name.replace('S ', ''))
    final_count = amount_correct/overall_amount
    # prints percent correct
    print(overall_amount)
    print(final_count)


# function that checks which answer tab (GAAP or STAT) to compare simulated answers to. This is needed as a field might
#  have a different transformation and ouput for GAAP and STAT
def result_checker(current_frame, trail, stat_data, gaap_data, check_data):
    if trail == 'gaapstat':
        gaap_check = tab_check(gaap_data, current_frame['GAAP Actual Output'][0], check_data, 'gaap')
        stat_check = tab_check(stat_data, current_frame['STAT Actual Output'][0], check_data, 'stat')
        if gaap_check == 1 and stat_check == 1:
            return 1
        else:
            return 0
    if trail == 'gaap':
        gaap_check = tab_check(gaap_data, current_frame['GAAP Actual Output'][0], check_data, trail)
        return gaap_check
    if trail == 'stat':
        stat_check = tab_check(stat_data, current_frame['STAT Actual Output'][0], check_data, trail)
        return stat_check


# function to assign the field name that GAAP and STAT use for policy number
def tab_check(data, actual_output_name, check_data, trail):
    # CNTRC_ID GAAP
    # POLICY STAT

    # just checks GAAP if transformation is the same between STAT and GAAP
    if trail == 'gaap':
        policy_name = 'CNTRC_ID'
    else:
        policy_name = 'POLICY'
    check_data = check_data.sort_index()
    actual_output_data = pd.Series(data=data[actual_output_name.replace(' S', '')].values, index=list(data[policy_name]))

    if check_data.equals(actual_output_data.sort_index()):
        return 1
    else:
        return 0


# function to write to output file
def write(check_dataframe, current_trail, save_path):
    book = load_workbook(save_path)
    writer = pd.ExcelWriter(save_path, engine='openpyxl')
    writer.book = book  # sets writer to that excel file
    check_dataframe.to_excel(writer, index=True, sheet_name=current_trail + ' Output')
    writer.save()  # saves excel


# main function. Update output file path here
def main():
    save_path = 'C:\\Users\\td753lx\\Desktop\\Prototype Output.xlsx'  # sets output file to write to and opens
    prototype, raw_data = import_prototype_data()
    current_value_set(prototype, raw_data, save_path)


if __name__ == '__main__':
    main()
