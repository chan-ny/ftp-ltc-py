import os
import pandas as pd
from datetime import datetime, timedelta

folder_path = '/Users/isdd/Desktop/DATA SOURCE/20240923'

# common data
common_columns = ["CDR_ID",
    "CDR_SUB_ID",
    "CDR_TYPE",
    "SPLIT_CDR_REASON",
    "CDR_BATCH_ID",
    "SRC_REC_LINE_NO",
    "SRC_CDR_ID",
    "SRC_CDR_NO",
    "STATUS",
    "RE_RATING_TIMES",
    "CREATE_DATE",
    "START_DATE",
    "END_DATE",
    "CUST_LOCAL_START_DATE",
    "CUST_LOCAL_END_DATE",
    "STD_EVT_TYPE_ID",
    "EVT_SOURCE_CATEGORY",
    "OBJ_TYPE",
    "OBJ_ID",
    "OWNER_CUST_ID",
    "DEFAULT_ACCT_ID",
    "PRI_IDENTITY",
    "BILL_CYCLE_ID",
    "SERVICE_CATEGORY",
    "USAGE_SERVICE_TYPE",
    "SESSION_ID",
    "RESULT_CODE",
    "RESULT_REASON",
    "BE_ID",
    "HOT_SEQ",
    "CP_ID",
    "RECIPIENT_NUMBER",
    "USAGE_MEASURE_ID",
    "ACTUAL_USAGE",
    "RATE_USAGE",
    "SERVICE_UNIT_TYPE",
    "USAGE_MEASURE_ID2",
    "ACTUAL_USAGE2",
    "RATE_USAGE2",
    "SERVICE_UNIT_TYPE2",
    "DEBIT_AMOUNT",
    "Reserved_1",
    "DEBIT_FROM_PREPAID",
    "DEBIT_FROM_ADVANCE_PREPAID",
    "DEBIT_FROM_POSTPAID",
    "DEBIT_FROM_ADVANCE_POSTPAID",
    "DEBIT_FROM_CREDIT_POSTPAID",
    "TOTAL_TAX",
    "FREE_UNIT_AMOUNT_OF_TIMES",
    "FREE_UNIT_AMOUNT_OF_DURATION",
    "FREE_UNIT_AMOUNT_OF_FLUX",
    "ACCT_ID_1",
    "ACCT_BALANCE_ID_1",
    "BALANCE_TYPE_1",
    "CUR_BALANCE_1",
    "CHG_BALANCE_1",
    "CURRENCY_ID_1",
    "OPER_TYPE_1_ACC",
    "ACCT_ID_2",
    "ACCT_BALANCE_ID_2",
    "BALANCE_TYPE_2",
    "CUR_BALANCE_2",
    "CHG_BALANCE_2",
    "CURRENCY_ID_2",
    "OPER_TYPE_2_ACC",
    "ACCT_ID_3",
    "ACCT_BALANCE_ID_3",
    "BALANCE_TYPE_3",
    "CUR_BALANCE_3",
    "CHG_BALANCE_3",
    "CURRENCY_ID_3",
    "OPER_TYPE_3_ACC",
    "ACCT_ID_4",
    "ACCT_BALANCE_ID_4",
    "BALANCE_TYPE_4",
    "CUR_BALANCE_4",
    "CHG_BALANCE_4",
    "CURRENCY_ID_4",
    "OPER_TYPE_4_ACC",
    "ACCT_ID_5",
    "ACCT_BALANCE_ID_5",
    "BALANCE_TYPE_5",
    "CUR_BALANCE_5",
    "CHG_BALANCE_5",
    "CURRENCY_ID_5",
    "OPER_TYPE_5_ACC",
    "ACCT_ID_6",
    "ACCT_BALANCE_ID_6",
    "BALANCE_TYPE_6",
    "CUR_BALANCE_6",
    "CHG_BALANCE_6",
    "CURRENCY_ID_6",
    "OPER_TYPE_6_ACC",
    "ACCT_ID_7",
    "ACCT_BALANCE_ID_7",
    "BALANCE_TYPE_7",
    "CUR_BALANCE_7",
    "CHG_BALANCE_7",
    "CURRENCY_ID_7",
    "OPER_TYPE_7_ACC",
    "ACCT_ID_8",
    "ACCT_BALANCE_ID_8",
    "BALANCE_TYPE_8",
    "CUR_BALANCE_8",
    "CHG_BALANCE_8",
    "CURRENCY_ID_8",
    "OPER_TYPE_8_ACC",
    "ACCT_ID_9",
    "ACCT_BALANCE_ID_9",
    "BALANCE_TYPE_9",
    "CUR_BALANCE_9",
    "CHG_BALANCE_9",
    "CURRENCY_ID_9",
    "OPER_TYPE_9_ACC",
    "ACCT_ID_10",
    "ACCT_BALANCE_ID_10",
    "BALANCE_TYPE_10",
    "CUR_BALANCE_10",
    "CHG_BALANCE_10",
    "CURRENCY_ID_10",
    "OPER_TYPE_10_ACC",
    "FU_OWN_TYPE",
    "FU_OWN_ID",
    "FREE_UNIT_ID",
    "FREE_UNIT_TYPE",
    "CUR_AMOUNT",
    "CHG_AMOUNT",
    "FU_MEASURE_ID",
    "OPER_TYPE",
    "FU_OWN_TYPE_1",
    "FU_OWN_ID_1",
    "FREE_UNIT_ID_1",
    "FREE_UNIT_TYPE_1",
    "CUR_AMOUNT_1",
    "CHG_AMOUNT_1",
    "FU_MEASURE_ID_1",
    "OPER_TYPE_1_FU",
    "FU_OWN_TYPE_2",
    "FU_OWN_ID_2",
    "FREE_UNIT_ID_2",
    "FREE_UNIT_TYPE_2",
    "CUR_AMOUNT_2",
    "CHG_AMOUNT_2",
    "FU_MEASURE_ID_2",
    "OPER_TYPE_2_FU",
    "FU_OWN_TYPE_3",
    "FU_OWN_ID_3",
    "FREE_UNIT_ID_3",
    "FREE_UNIT_TYPE_3",
    "CUR_AMOUNT_3",
    "CHG_AMOUNT_3",
    "FU_MEASURE_ID_3",
    "OPER_TYPE_3_FU",
    "FU_OWN_TYPE_4",
    "FU_OWN_ID_4",
    "FREE_UNIT_ID_4",
    "FREE_UNIT_TYPE_4",
    "CUR_AMOUNT_4",
    "CHG_AMOUNT_4",
    "FU_MEASURE_ID_4",
    "OPER_TYPE_4_FU",
    "FU_OWN_TYPE_5",
    "FU_OWN_ID_5",
    "FREE_UNIT_ID_5",
    "FREE_UNIT_TYPE_5",
    "CUR_AMOUNT_5",
    "CHG_AMOUNT_5",
    "FU_MEASURE_ID_5",
    "OPER_TYPE_5_FU",
    "FU_OWN_TYPE_6",
    "FU_OWN_ID_6",
    "FREE_UNIT_ID_6",
    "FREE_UNIT_TYPE_6",
    "CUR_AMOUNT_6",
    "CHG_AMOUNT_6",
    "FU_MEASURE_ID_6",
    "OPER_TYPE_6_FU",
    "FU_OWN_TYPE_7",
    "FU_OWN_ID_7",
    "FREE_UNIT_ID_7",
    "FREE_UNIT_TYPE_7",
    "CUR_AMOUNT_7",
    "CHG_AMOUNT_7",
    "FU_MEASURE_ID_7",
    "OPER_TYPE_7_FU",
    "FU_OWN_TYPE_8",
    "FU_OWN_ID_8",
    "FREE_UNIT_ID_8",
    "FREE_UNIT_TYPE_8",
    "CUR_AMOUNT_8",
    "CHG_AMOUNT_8",
    "FU_MEASURE_ID_8",
    "OPER_TYPE_8_FU",
    "FU_OWN_TYPE_9",
    "FU_OWN_ID_9",
    "FREE_UNIT_ID_9",
    "FREE_UNIT_TYPE_9",
    "CUR_AMOUNT_9",
    "CHG_AMOUNT_9",
    "FU_MEASURE_ID_9",
    "OPER_TYPE_9_FU",
    "ACCT_ID_11",
    "ACCT_BALANCE_ID_11",
    "BALANCE_TYPE_11",
    "BONUS_AMOUNT_11_ACC",
    "CURRENT_BALANCE_11",
    "CURRENCY_ID_11",
    "OPER_TYPE_11_ACC",
    "ACCT_ID_12",
    "ACCT_BALANCE_ID_12",
    "BALANCE_TYPE_12",
    "BONUS_AMOUNT_12_ACC",
    "CURRENT_BALANCE_12",
    "CURRENCY_ID_12",
    "OPER_TYPE_12_ACC",
    "ACCT_ID_13",
    "ACCT_BALANCE_ID_13",
    "BALANCE_TYPE_13",
    "BONUS_AMOUNT_13_ACC",
    "CURRENT_BALANCE_13",
    "CURRENCY_ID_13",
    "OPER_TYPE_13_ACC",
    "ACCT_ID_14",
    "ACCT_BALANCE_ID_14",
    "BALANCE_TYPE_14",
    "BONUS_AMOUNT_14_ACC",
    "CURRENT_BALANCE_14",
    "CURRENCY_ID_14",
    "OPER_TYPE_14_ACC",
    "ACCT_ID_15",
    "ACCT_BALANCE_ID_15",
    "BALANCE_TYPE_15",
    "BONUS_AMOUNT_15_ACC",
    "CURRENT_BALANCE_15",
    "CURRENCY_ID_15",
    "OPER_TYPE_15_ACC",
    "ACCT_ID_16",
    "ACCT_BALANCE_ID_16",
    "BALANCE_TYPE_16",
    "BONUS_AMOUNT_16_ACC",
    "CURRENT_BALANCE_16",
    "CURRENCY_ID_16",
    "OPER_TYPE_16_ACC",
    "ACCT_ID_17",
    "ACCT_BALANCE_ID_17",
    "BALANCE_TYPE_17",
    "BONUS_AMOUNT_17_ACC",
    "CURRENT_BALANCE_17",
    "CURRENCY_ID_17",
    "OPER_TYPE_17_ACC",
    "ACCT_ID_19",
    "ACCT_BALANCE_ID_19",
    "BALANCE_TYPE_19",
    "BONUS_AMOUNT_19_ACC",
    "CURRENT_BALANCE_19",
    "CURRENCY_ID_19",
    "OPER_TYPE_19_ACC",
    "ACCT_ID_20",
    "ACCT_BALANCE_ID_20",
    "BALANCE_TYPE_20",
    "BONUS_AMOUNT_20",
    "CURRENT_BALANCE_20",
    "CURRENCY_ID_20",
    "OPER_TYPE_20",
    "ACCT_ID_21",
    "ACCT_BALANCE_ID_21",
    "BALANCE_TYPE_21",
    "BONUS_AMOUNT_21",
    "CURRENT_BALANCE_21",
    "CURRENCY_ID_21",
    "OPER_TYPE_21",
    "FU_OWN_TYPE_10",
    "FU_OWN_ID_10",
    "FREE_UNIT_TYPE_10",
    "FREE_UNIT_ID_10",
    "BONUS_AMOUNT_10",
    "CURRENT_AMOUNT_10",
    "FU_MEASURE_ID_10",
    "OPER_TYPE_10_FU",
    "FU_OWN_TYPE_11",
    "FU_OWN_ID_11",
    "FREE_UNIT_TYPE_11",
    "FREE_UNIT_ID_11",
    "BONUS_AMOUNT_11_FU",
    "CURRENT_AMOUNT_11",
    "FU_MEASURE_ID_11",
    "OPER_TYPE_11_FU",
    "FU_OWN_TYPE_12",
    "FU_OWN_ID_12",
    "FREE_UNIT_TYPE_12",
    "FREE_UNIT_ID_12",
    "BONUS_AMOUNT_12_FU",
    "CURRENT_AMOUNT_12",
    "FU_MEASURE_ID_12",
    "OPER_TYPE_12_FU",
    "FU_OWN_TYPE_13",
    "FU_OWN_ID_13",
    "FREE_UNIT_TYPE_13",
    "FREE_UNIT_ID_13",
    "BONUS_AMOUNT_13_FU",
    "CURRENT_AMOUNT_13",
    "FU_MEASURE_ID_13",
    "OPER_TYPE_13_FU",
    "FU_OWN_TYPE_14",
    "FU_OWN_ID_14",
    "FREE_UNIT_TYPE_14",
    "FREE_UNIT_ID_14",
    "BONUS_AMOUNT_14_FU",
    "CURRENT_AMOUNT_14",
    "FU_MEASURE_ID_14",
    "OPER_TYPE_14_FU",
    "FU_OWN_TYPE_15",
    "FU_OWN_ID_15",
    "FREE_UNIT_TYPE_15",
    "FREE_UNIT_ID_15",
    "BONUS_AMOUNT_15_FU",
    "CURRENT_AMOUNT_15",
    "FU_MEASURE_ID_15",
    "OPER_TYPE_15_FU",
    "FU_OWN_TYPE_16",
    "FU_OWN_ID_16",
    "FREE_UNIT_TYPE_16",
    "FREE_UNIT_ID_16",
    "BONUS_AMOUNT_16_FU",
    "CURRENT_AMOUNT_16",
    "FU_MEASURE_ID_16",
    "OPER_TYPE_16_FU",
    "FU_OWN_TYPE_17",
    "FU_OWN_ID_17",
    "FREE_UNIT_TYPE_17",
    "FREE_UNIT_ID_17",
    "BONUS_AMOUNT_17_FU",
    "CURRENT_AMOUNT_17",
    "FU_MEASURE_ID_17",
    "OPER_TYPE_17_FU",
    "FU_OWN_TYPE_18",
    "FU_OWN_ID_18",
    "FREE_UNIT_TYPE_18",
    "FREE_UNIT_ID_18",
    "BONUS_AMOUNT_18",
    "CURRENT_AMOUNT_18",
    "FU_MEASURE_ID_18",
    "OPER_TYPE_18",
    "FU_OWN_TYPE_19",
    "FU_OWN_ID_19",
    "FREE_UNIT_TYPE_19",
    "FREE_UNIT_ID_19",
    "BONUS_AMOUNT_19_FU",
    "CURRENT_AMOUNT_19",
    "FU_MEASURE_ID_19",
    "OPER_TYPE_19_FU"]
# List all files in the folder
def filter_file (keyword):
    matched_files = []
    for root, dirs, files in os.walk(folder_path):
        # Check each file to see if the keyword is in the file name
        for file in files:
            if keyword in file:
                matched_files.append(os.path.join(root, file))
    
    return matched_files

# Adjust data types
def optimize_datatypes(df):
    # Convert string-like columns to 'category' for memory efficiency
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype('category')

    for col in df.select_dtypes(include=['int']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    return df

# write parquet file
def write_parquet(df, output_file):
    
    chunk_size = 100
    for idx, chunk in enumerate(range(0, len(df), chunk_size)):
        df = df.iloc[chunk:chunk + chunk_size]
        df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=True)
        
def has_duplicates(arr):
    seen = set()
    duplicates = set()
    for item in arr:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    return list(duplicates)

# voice filter data
def voice_data():
    data = filter_file('ltc_pps_cbs_cdr_voice')
    item_columns = ["CallingPartyNumber",
    "CalledPartyNumber",
    "CallingPartyIMSI",
    "CalledPartyIMSI",
    "DialedNumber",
    "OriginalCalledParty",
    "ServiceFlow",
    "CallForwardIndicator",
    "CallingRoamInfo",
    "CallingCellID",
    "CalledRoamInfo",
    "CalledCellID",
    "TimeStampOfSSP",
    "TimeZoneOfSSP",
    "BearerCapability",
    "ChargingTime",
    "WaitDuration",
    "TerminationReason",
    "CallReferenceNumber",
    "IMEI",
    "AccessPrefix",
    "RoutingPrefix",
    "RedirectingPartyID",
    "MSCAddress",
    "BrandID",
    "MainOfferingID",
    "ChargingPartyNumber",
    "ChargePartyIndicator",
    "PayType",
    "ChargingType",
    "CallType",
    "RoamState",
    "CallingHomeCountryCode",
    "CallingHomeAreaNumber",
    "CallingHomeNetworkCode",
    "CallingRoamCountryCode",
    "CallingRoamAreaNumber",
    "CallingRoamNetworkCode",
    "CalledHomeCountryCode",
    "CalledHomeAreaNumber",
    "CalledHomeNetworkCode",
    "CalledRoamCountryCode",
    "CalledRoamAreaNumber",
    "CalledRoamNetworkCode",
    "ServiceType",
    "HotLineIndicator",
    "HomeZoneID",
    "SpecialZoneID",
    "NPFlag",
    "NPPrefix",
    "CallingCUGNo",
    "CalledCUGNo",
    "OpposeNumberType",
    "CallingNetworkType",
    "CalledNetworkType",
    "CallingVPNTopGroupNumber",
    "CallingVPNGroupNumber",
    "CallingVPNShortNumber",
    "CalledVPNTopGroupNumber",
    "CalledVPNGroupNumber",
    "CalledVPNShortNumber",
    "GroupCallType",
    "OnlineChargingFlag",
    "StartTimeOfBillCycle",
    "LastEffectOffering",
    "DTDiscount",
    "OpposeMainOfferingID",
    "MainBalanceInfo",
    "ChgBalanceInfo",
    "ChgFreeUnitInfo",
    "UserState",
    "GroupPayFlag",
    "RoamingZoneID",
    "PrimaryOfferChgAmt",
    "OriginatingIOI",
    "TerminatingIOI",
    "IMSChargingIdentifier",
    "Reserved_2",
    "RECEPIENT_CP_ID",
    "TaxInfo",
    "VistLocalStartTime",
    "VistLocalEndTime",
    "SUBSCRIBER_KEY",
    "ACCOUNT_KEY",
    "OWNER_CUST_CODE"]
    concat_common_voice_columns = common_columns + item_columns
    
    # loop data
    count = 1
    data_voice = []
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')
                
                
                for line in content_split:
                    if len(line.split('|')) > 3:
                        data_voice.append(line.split('|')[:-2])
                
                # make file parquet
                df = pd.DataFrame(data_voice, columns=concat_common_voice_columns)
                df = df.set_index(['PRI_IDENTITY', 'CDR_BATCH_ID','CREATE_DATE', 'START_DATE', 'END_DATE', 'RECIPIENT_NUMBER', 'CalledPartyNumber', 'CallingPartyNumber', 'TimeStampOfSSP', 'ChargingTime'])
                df = optimize_datatypes(df)
                
                # path output
                current_datetime = datetime.now()
                one_day_ago = current_datetime - timedelta(days=1)
                formatted_datetime = one_day_ago.strftime("%Y%m%d%H%M%S.%f")
                file_path = f'/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/voices/ltc_cdr_cbs_voice_{formatted_datetime}_{count}.parquet'
                write_parquet(df, file_path)
                print(f'{file_name} is ok')
        count += 1
    
# SMS filter data
def sms_data():
    data = filter_file('ltc_pps_cbs_cdr_sms')
    item_columns = [
        "CallingPartyNumber",
        "CalledPartyNumber",
        "CallingPartyIMSI",
        "CalledPartyIMSI",
        "DialedNumber",
        "OriginalCalledParty",
        "ServiceFlow",
        "CallForwardIndicator",
        "CallingRoamInfo",
        "CallingCellID",
        "CalledRoamInfo",
        "CalledCellID",
        "TimeStampOfSSP",
        "TimeZoneOfSSP",
        "BearerCapability",
        "ChargingTime",
        "SendResult",
        "SMSID",
        "IMEI",
        "SMLength",
        "SMSCAddress",
        "RefundIndicator",
        "BrandID",
        "MainOfferingID",
        "ChargingPartyNumber",
        "ChargePartyIndicator",
        "PayType",
        "ChargingType",
        "SMSType",
        "OnNetIndicator",
        "RoamState",
        "CallingHomeCountryCode",
        "CallingHomeAreaNumber",
        "CallingHomeNetworkCode",
        "CallingRoamCountryCode",
        "CallingRoamAreaNumber",
        "CallingRoamNetworkCode",
        "CalledHomeCountryCode",
        "CalledHomeNetworkCode",
        "CalledRoamCountryCode",
        "CalledRoamAreaNumber",
        "CalledRoamNetworkCode",
        "ServiceType",
        "SpecialNumberIndicator",
        "NPFlag",
        "NPPrefix",
        "CallingCUGNo",
        "CalledCUGNo",
        "OpposeNetworkType",
        "OpposeNumberType",
        "CallingNetworkType",
        "CalledNetworkType",
        "CallingVPNTopGroupNumber",
        "CallingVPNGroupNumber",
        "CallingVPNShortNumber",
        "CalledVPNTopGroupNumber",
        "CalledVPNGroupNumber",
        "CalledVPNShortNumber",
        "GroupCallType",
        "OnlineChargingFlag",
        "StartTimeOfBillCycle",
        "LastEffectOffering",
        "DTDiscount",
        "OpposeMainOfferingID",
        "HomeZoneID",
        "SpecialZoneID",
        "MainBalanceInfo",
        "ChgBalanceInfo",
        "ChgFreeUnitInfo",
        "UserState",
        "GroupPayFlag",
        "RoamingZoneID",
        "PrimaryOfferChgAmt",
        "Reserved",
        "RECEPIENT_CP_ID",
        "TaxInfo",
        "VistLocalStartTime",
        "VistLocalEndTime",
        "SUBSCRIBER_KEY",
        "ACCOUNT_KEY",
        "OWNER_CUST_CODE",
        "CallingLocationNumber"]
    concat_common_voice_columns = common_columns + item_columns
    
    # loop data
    count = 1
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')
                
                data_voice = []
                for line in content_split:
                    if len(line.split('|')) > 3:
                        data_voice.append(line.split('|')[:-2])
                
                # make file parquet
                df = pd.DataFrame(data_voice, columns=concat_common_voice_columns)
                df = df.set_index(['PRI_IDENTITY', 'CDR_BATCH_ID','CREATE_DATE', 'START_DATE', 'END_DATE', 'RECIPIENT_NUMBER', 'CalledPartyNumber', 'CallingPartyNumber', 'TimeStampOfSSP', 'ChargingTime'])
                df = optimize_datatypes(df)
                
                # path output
                current_datetime = datetime.now()
                one_day_ago = current_datetime - timedelta(days=1)
                formatted_datetime = one_day_ago.strftime("%Y%m%d%H%M%S.%f")
                file_path = f'/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/sms/ltc_cdr_cbs_sms_{formatted_datetime}_{count}.parquet'
                write_parquet(df, file_path)
                print(f'{file_name} is ok')
        count += 1

# Recurring data
def recurring_data():
    data = filter_file('ltc_pps_cbs_cdr_mon')
    item_columns = [
        "BrandID",
        "MainOfferingID",
        "PayType",
        "ChargingPartyNumber",
        "ChargingPartyType",
        "CycleBeginTime",
        "CycleEndTime",
        "CycleType",
        "CycleLength",
        "ElapseCycles",
        "InnerCycleBeginTime",
        "InnerCycleEndTime",
        "ProductID",
        "OfferingID",
        "OfferingInstID",
        "OrderStatus",
        "ProrateFlag",
        "StartTimeOfBillCycle",
        "ChargePartyIndicator",
        "PosRentMode",
        "TriggerMode",
        "ExecuteFlag",
        "ProductCode",
        "MainBalanceInfo",
        "ChgBalanceInfo",
        "ChgFreeUnitInfo",
        "UserState",
        "CallingVPNTopGroupNumber",
        "CallingVPNGroupNumber",
        "GroupPayFlag",
        "TaxInfo",
        "LegacyOfferCode",
        "PurchaseSequence",
        "SUBSCRIBER_KEY",
        "ACCOUNT_KEY",
        "OWNER_CUST_CODE"]
    concat_common_voice_columns = common_columns + item_columns
    
    # loop data
    count = 1
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')
                
                data_voice = []
                for line in content_split:
                    if len(line.split('|')) > 3:
                        data_voice.append(line.split('|')[:-1])
                
                # make file parquet
                df = pd.DataFrame(data_voice, columns=concat_common_voice_columns)
                df = df.set_index(['PRI_IDENTITY', 'CDR_BATCH_ID','CREATE_DATE', 'START_DATE', 'END_DATE', 'ChargingPartyNumber', 'CycleBeginTime', 'InnerCycleEndTime'])
                df = optimize_datatypes(df)
                
                # path output
                current_datetime = datetime.now()
                one_day_ago = current_datetime - timedelta(days=1)
                formatted_datetime = one_day_ago.strftime("%Y%m%d%H%M%S.%f")
                file_path = f'/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/recurring/ltc_cdr_cbs_mon_{formatted_datetime}_{count}.parquet'
                write_parquet(df, file_path)
                print(f'{file_name} is ok')
        count += 1

# Management Data
def Management_data():
    data = filter_file('ltc_pps_cbs_cdr_cm')
    item_columns = [
        "CallingPartyNumber",
        "OperationID",
        "OperationType",
        "BrandID",
        "MainOfferingID",
        "PayType",
        "StartTimeOfBillCycle",
        "Reserve",
        "Merchant",
        "Service",
        "MainBalanceInfo",
        "ChgBalanceInfo",
        "ChgFreeUnitInfo",
        "UserState",
        "OldUserState",
        "ValidDayAdded",
        "Oper_ID",
        "SPID",
        "ServiceID",
        "ServiceType",
        "ContentID",
        "url",
        "TransactionID",
        "OldMainOfferingID",
        "AdditionalInfo",
        "NewOfferingID",
        "ExpireTimeofNewOffering",
        "ChargePartyIndicator",
        "AccessMethod",
        "TaxInfo",
        "ACCOUNT_KEY",
        "OWNER_CUST_CODE",
        "SUBSCRIBER_KEY"]
    concat_common_voice_columns = common_columns + item_columns
    
    # loop data
    count = 1
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')
                
                data_voice = []
                for line in content_split:
                    if len(line.split('|')) > 3:
                        data_voice.append(line.split('|')[:-1])
                
                # make file parquet
                df = pd.DataFrame(data_voice, columns=concat_common_voice_columns)
                df = df.set_index(['PRI_IDENTITY', 'CDR_BATCH_ID', 'CREATE_DATE', 'START_DATE', 'END_DATE', 'StartTimeOfBillCycle'])
                df = optimize_datatypes(df)
                
                # path output
                current_datetime = datetime.now()
                one_day_ago = current_datetime - timedelta(days=1)
                formatted_datetime = one_day_ago.strftime("%Y%m%d%H%M%S.%f")
                file_path = f'/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/management/ltc_cdr_cbs_cm_{formatted_datetime}_{count}.parquet'
                write_parquet(df, file_path)
                print(f'{file_path} is ok')
        count += 1
     
     
def save_dataframe(data_items, columns_items, columns_index, file_name):
    # make file parquet
    df = pd.DataFrame(data_items, columns=columns_items)
    df = df.set_index(columns_index)
    df = optimize_datatypes(df)
    
    # path output
    current_datetime = datetime.now()
    one_day_ago = current_datetime - timedelta(days=1)
    formatted_datetime = one_day_ago.strftime("%Y%m%d%H%M%S.%f")
    file_path = f'{file_name}_{formatted_datetime}.parquet'
    write_parquet(df, file_path)
    print(f'{file_path} is ok')

# Resource Clearance
def Resource_clearance_data():
    data = filter_file('ltc_pps_cbs_cdr_clr')
    item_columns = [
        "BrandID",
        "PrimaryOfferingID",
        "PaymentType",
        "UserState"]
    concat_common_voice_columns = common_columns + item_columns
    
    file_path ='/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/resource_clearance/ltc_cdr_cbs_clr'
    columns_index = ['PRI_IDENTITY', 'CDR_BATCH_ID', 'CREATE_DATE', 'START_DATE', 'END_DATE']
    
    # loop data
    data_items = []
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')[:-1]
                
                for line in content_split:
                    if len(line.split('|')) > 3:
                        if len(data_items) < 100:
                            data_items.append(line.split('|')[:-1])
                        else:
                            save_dataframe(data_items, concat_common_voice_columns, columns_index, file_path)
                            data_items = []
                            data_items.append(line.split('|')[:-1])
    # save data
    if len(data_items) > 0:
        save_dataframe(data_items, concat_common_voice_columns, columns_index, file_path)

# Resource Clearance
def first_time_activation_data():
    data = filter_file('ltc_pps_cbs_cdr_first')
    item_columns = [
        "BrandID",
        "PrimaryOfferingID",
        "PaymentType",
        "OldUserStat",
        "CurrUserStat",
        "BillCycleID",
        "ChgBalanceInfo",
        "ChgFreeUnitInfo",
        "MainBalanceInfo",
        "UserState",
        "OldUserState",
        "Oper_ID",
        "THIRD_PARTY_NUMBER",
        "EXT_TRANS_ID",
        "EXT_TRANS_TYPE",
        "ACCESS_METHOD",
        "RechargeCellID",
        "AgentName",
        "AdditionalInfo",
        "SubIdentityType"
    ]
    concat_common_voice_columns = common_columns + item_columns
    
    
    file_path ='/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/first_time_active/ltc_cdr_cbs_first'
    columns_index = ['PRI_IDENTITY', 'CDR_BATCH_ID', 'CREATE_DATE', 'START_DATE', 'END_DATE']
    
    # loop data
    data_items = []
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')[:-1]
                
                for line in content_split:
                    if len(line.split('|')) > 3:
                        if len(data_items) < 100:
                            data_items.append(line.split('|')[:-1])
                        else:
                            save_dataframe(data_items, concat_common_voice_columns, columns_index, file_path)
                            data_items = []
                            data_items.append(line.split('|')[:-1])
    # save data
    if len(data_items) > 0:
        save_dataframe(data_items, concat_common_voice_columns, columns_index, file_path)
        
def rename_duplicates(arr):
    element_count = {}
    renamed_array = []

    for element in arr:
        # Check if the element already exists in the dictionary
        if element in element_count:
            element_count[element] += 1
            # Rename by appending the count as suffix
            renamed_array.append(f"{element}_{element_count[element]}")
        else:
            element_count[element] = 0
            renamed_array.append(element)

    return renamed_array

# Recharge data
def Recharge_data():
    data = filter_file('ltc_pps_cbs_cdr_vou')
    item_columns = ['RECHARGE_LOG_ID', 'RECHARGE_CODE', 'RECHARGE_AMT', 'ACCT_ID', 'SUB_ID', 'PRI_IDENTITY', 'THIRD_PARTY_NUMBER', 'CURRENCY_ID', 'ORIGINAL_AMT', 'CURRENCY_RATE', 'CONVERSION_AMT', 'RECHARGE_TRANS_ID', 'EXT_TRANS_TYPE', 'EXT_TRANS_ID', 'ACCESS_METHOD', 'BATCH_NO', 'OFFERING_ID', 'PAYMENT_TYPE', 'RECHARGE_TAX', 'RECHARGE_PENALTY', 'RECHARGE_TYPE', 'CHANNEL_ID', 'RECHARGE_REASON', 'RESULT_CODE', 'ERROR_TYPE', 'VALID_DAY_ADDED', 'DIAMETER_SESSIONID', 'OPER_ID', 'DEPT_ID', 'ENTRY_DATE', 'RECON_DATE', 'RECON_STATUS', 'REVERSAL_TRANS_ID', 'REVERSAL_REASON_CODE', 'REVERSAL_OPER_ID', 'REVERSAL_DEPT_ID', 'REVERSAL_DATE', 'STATUS', 'REMARK', 'BE_ID', 'BE_CODE', 'REGION_ID', 'REGION_CODE', 'CARD_SEQUENCE', 'CARD_PIN_NUMBER', 'CARD_BATCH_NO', 'CARD_STATUS', 'CARD_COS_ID', 'CARD_SP_ID', 'CARD_AMOUNT', 'CARD_VALIDITY', 'VOUCHER_ENCRYPT_NUMBER', 'CHECK_NO', 'CHECK_DATE', 'CREDIT_CARD_NO', 'CREDIT_CARD_NAME', 'CREDIT_CARD_TYPE_ID', 'CC_EXPIRY_DATE', 'CC_AUTHORIZATION_CODE', 'BANK_CODE', 'BANK_BRANCH_CODE', 'ACCT_NO', 'BANK_ACCT_NAME', 'LOAN_AMOUNT', 'LOAN_POUNDATE', 'ACCT_ID_1', 'ACCT_BALANCE_ID', 'BALANCE_TYPE', 'CUR_BALANCE', 'CHG_BALANCE', 'PRE_APPLY_TIME', 'PRE_EXPIRE_TIME', 'CUR_EXPIRE_TIME', 'CURRENCYE_ID', 'OPER_TYPE', 'ACCT_ID_2', 'ACCT_BALANCE_ID_1', 'BALANCE_TYPE_1', 'CUR_BALANCE_1', 'CHG_BALANCE_1', 'PRE_APPLY_TIME_1', 'PRE_EXPIRE_TIME_1', 'CUR_EXPIRE_TIME_1', 'CURRENCYE_ID_1', 'OPER_TYPE_1', 'ACCT_ID_3', 'ACCT_BALANCE_ID_2', 'BALANCE_TYPE_2', 'CUR_BALANCE_2', 'CHG_BALANCE_2', 'PRE_APPLY_TIME_2', 'PRE_EXPIRE_TIME_2', 'CUR_EXPIRE_TIME_2', 'CURRENCYE_ID_2', 'OPER_TYPE_2', 'ACCT_ID_4', 'ACCT_BALANCE_ID_3', 'BALANCE_TYPE_3', 'CUR_BALANCE_3', 'CHG_BALANCE_3', 'PRE_APPLY_TIME_3', 'PRE_EXPIRE_TIME_3', 'CUR_EXPIRE_TIME_3', 'CURRENCYE_ID_3', 'OPER_TYPE_3', 'ACCT_ID_5', 'ACCT_BALANCE_ID_4', 'BALANCE_TYPE_4', 'CUR_BALANCE_4', 'CHG_BALANCE_4', 'PRE_APPLY_TIME_4', 'PRE_EXPIRE_TIME_4', 'CUR_EXPIRE_TIME_4', 'CURRENCYE_ID_4', 'OPER_TYPE_4', 'ACCT_ID_6', 'ACCT_BALANCE_ID_5', 'BALANCE_TYPE_5', 'CUR_BALANCE_5', 'CHG_BALANCE_5', 'PRE_APPLY_TIME_5', 'PRE_EXPIRE_TIME_5', 'CUR_EXPIRE_TIME_5', 'CURRENCYE_ID_5', 'OPER_TYPE_5', 'ACCT_ID_7', 'ACCT_BALANCE_ID_6', 'BALANCE_TYPE_6', 'CUR_BALANCE_6', 'CHG_BALANCE_6', 'PRE_APPLY_TIME_6', 'PRE_EXPIRE_TIME_6', 'CUR_EXPIRE_TIME_6', 'CURRENCYE_ID_6', 'OPER_TYPE_6', 'ACCT_ID_8', 'ACCT_BALANCE_ID_7', 'BALANCE_TYPE_7', 'CUR_BALANCE_7', 'CHG_BALANCE_7', 'PRE_APPLY_TIME_7', 'PRE_EXPIRE_TIME_7', 'CUR_EXPIRE_TIME_7', 'CURRENCYE_ID_7', 'OPER_TYPE_7', 'ACCT_ID_9', 'ACCT_BALANCE_ID_8', 'BALANCE_TYPE_8', 'CUR_BALANCE_8', 'CHG_BALANCE_8', 'PRE_APPLY_TIME_8', 'PRE_EXPIRE_TIME_8', 'CUR_EXPIRE_TIME_8', 'CURRENCYE_ID_8', 'OPER_TYPE_8', 'ACCT_ID_10', 'ACCT_BALANCE_ID_9', 'BALANCE_TYPE_9', 'CUR_BALANCE_9', 'CHG_BALANCE_9', 'PRE_APPLY_TIME_9', 'PRE_EXPIRE_TIME_9', 'CUR_EXPIRE_TIME_9', 'CURRENCYE_ID_9', 'OPER_TYPE_9', 'FU_OWN_TYPE', 'FU_OWN_ID', 'FREE_UNIT_ID', 'FREE_UNIT_TYPE', 'CUR_AMOUNT', 'CHG_AMOUNT', 'PRE_APPLY_TIME_10', 'PRE_EXPIRE_TIME_10', 'CUR_EXPIRE_TIME_10', 'FU_MEASURE_ID', 'OPER_TYPE_10', 'FU_OWN_TYPE_1', 'FU_OWN_ID_1', 'FREE_UNIT_ID_1', 'FREE_UNIT_TYPE_1', 'CUR_AMOUNT_1', 'CHG_AMOUNT_1', 'PRE_APPLY_TIME_11', 'PRE_EXPIRE_TIME_11', 'CUR_EXPIRE_TIME_11', 'FU_MEASURE_ID_1', 'OPER_TYPE_11', 'FU_OWN_TYPE_2', 'FU_OWN_ID_2', 'FREE_UNIT_ID_2', 'FREE_UNIT_TYPE_2', 'CUR_AMOUNT_2', 'CHG_AMOUNT_2', 'PRE_APPLY_TIME_12', 'PRE_EXPIRE_TIME_12', 'CUR_EXPIRE_TIME_12', 'FU_MEASURE_ID_2', 'OPER_TYPE_12', 'FU_OWN_TYPE_3', 'FU_OWN_ID_3', 'FREE_UNIT_ID_3', 'FREE_UNIT_TYPE_3', 'CUR_AMOUNT_3', 'CHG_AMOUNT_3', 'PRE_APPLY_TIME_13', 'PRE_EXPIRE_TIME_13', 'CUR_EXPIRE_TIME_13', 'FU_MEASURE_ID_3', 'OPER_TYPE_13', 'FU_OWN_TYPE_4', 'FU_OWN_ID_4', 'FREE_UNIT_ID_4', 'FREE_UNIT_TYPE_4', 'CUR_AMOUNT_4', 'CHG_AMOUNT_4', 'PRE_APPLY_TIME_14', 'PRE_EXPIRE_TIME_14', 'CUR_EXPIRE_TIME_14', 'FU_MEASURE_ID_4', 'OPER_TYPE_14', 'FU_OWN_TYPE_5', 'FU_OWN_ID_5', 'FREE_UNIT_ID_5', 'FREE_UNIT_TYPE_5', 'CUR_AMOUNT_5', 'CHG_AMOUNT_5', 'PRE_APPLY_TIME_15', 'PRE_EXPIRE_TIME_15', 'CUR_EXPIRE_TIME_15', 'FU_MEASURE_ID_5', 'OPER_TYPE_15', 'FU_OWN_TYPE_6', 'FU_OWN_ID_6', 'FREE_UNIT_ID_6', 'FREE_UNIT_TYPE_6', 'CUR_AMOUNT_6', 'CHG_AMOUNT_6', 'PRE_APPLY_TIME_16', 'PRE_EXPIRE_TIME_16', 'CUR_EXPIRE_TIME_16', 'FU_MEASURE_ID_6', 'OPER_TYPE_16', 'FU_OWN_TYPE_7', 'FU_OWN_ID_7', 'FREE_UNIT_ID_7', 'FREE_UNIT_TYPE_7', 'CUR_AMOUNT_7', 'CHG_AMOUNT_7', 'PRE_APPLY_TIME_17', 'PRE_EXPIRE_TIME_17', 'CUR_EXPIRE_TIME_17', 'FU_MEASURE_ID_7', 'OPER_TYPE_17', 'FU_OWN_TYPE_8', 'FU_OWN_ID_8', 'FREE_UNIT_ID_8', 'FREE_UNIT_TYPE_8', 'CUR_AMOUNT_8', 'CHG_AMOUNT_8', 'PRE_APPLY_TIME_18', 'PRE_EXPIRE_TIME_18', 'CUR_EXPIRE_TIME_18', 'FU_MEASURE_ID_8', 'OPER_TYPE_18', 'FU_OWN_TYPE_9', 'FU_OWN_ID_9', 'FREE_UNIT_ID_9', 'FREE_UNIT_TYPE_9', 'CUR_AMOUNT_9', 'CHG_AMOUNT_9', 'PRE_APPLY_TIME_19', 'PRE_EXPIRE_TIME_19', 'CUR_EXPIRE_TIME_19', 'FU_MEASURE_ID_9', 'OPER_TYPE_19', 'ACCT_ID_11', 'ACCT_BALANCE_ID_10', 'BALANCE_TYPE_10', 'BONUS_AMOUNT', 'CURRENT_BALANCE', 'PRE_APPLY_TIME_20', 'PRE_EXPIRE_TIME_20', 'CUR_EXPIRE_TIME_20', 'CURRENCY_ID_1', 'OPER_TYPE_20', 'ACCT_ID_12', 'ACCT_BALANCE_ID_11', 'BALANCE_TYPE_11', 'BONUS_AMOUNT_1', 'CURRENT_BALANCE_1', 'PRE_APPLY_TIME_21', 'PRE_EXPIRE_TIME_21', 'CUR_EXPIRE_TIME_21', 'CURRENCY_ID_2', 'OPER_TYPE_21', 'ACCT_ID_13', 'ACCT_BALANCE_ID_12', 'BALANCE_TYPE_12', 'BONUS_AMOUNT_2', 'CURRENT_BALANCE_2', 'PRE_APPLY_TIME_22', 'PRE_EXPIRE_TIME_22', 'CUR_EXPIRE_TIME_22', 'CURRENCY_ID_3', 'OPER_TYPE_22', 'ACCT_ID_14', 'ACCT_BALANCE_ID_13', 'BALANCE_TYPE_13', 'BONUS_AMOUNT_3', 'CURRENT_BALANCE_3', 'PRE_APPLY_TIME_23', 'PRE_EXPIRE_TIME_23', 'CUR_EXPIRE_TIME_23', 'CURRENCY_ID_4', 'OPER_TYPE_23', 'ACCT_ID_15', 'ACCT_BALANCE_ID_14', 'BALANCE_TYPE_14', 'BONUS_AMOUNT_4', 'CURRENT_BALANCE_4', 'PRE_APPLY_TIME_24', 'PRE_EXPIRE_TIME_24', 'CUR_EXPIRE_TIME_24', 'CURRENCY_ID_5', 'OPER_TYPE_24', 'ACCT_ID_16', 'ACCT_BALANCE_ID_15', 'BALANCE_TYPE_15', 'BONUS_AMOUNT_5', 'CURRENT_BALANCE_5', 'PRE_APPLY_TIME_25', 'PRE_EXPIRE_TIME_25', 'CUR_EXPIRE_TIME_25', 'CURRENCY_ID_6', 'OPER_TYPE_25', 'ACCT_ID_17', 'ACCT_BALANCE_ID_16', 'BALANCE_TYPE_16', 'BONUS_AMOUNT_6', 'CURRENT_BALANCE_6', 'PRE_APPLY_TIME_26', 'PRE_EXPIRE_TIME_26', 'CUR_EXPIRE_TIME_26', 'CURRENCY_ID_7', 'OPER_TYPE_26', 'ACCT_ID_18', 'ACCT_BALANCE_ID_17', 'BALANCE_TYPE_17', 'BONUS_AMOUNT_7', 'CURRENT_BALANCE_7', 'PRE_APPLY_TIME_27', 'PRE_EXPIRE_TIME_27', 'CUR_EXPIRE_TIME_27', 'CURRENCY_ID_8', 'OPER_TYPE_27', 'ACCT_ID_19', 'ACCT_BALANCE_ID_18', 'BALANCE_TYPE_18', 'BONUS_AMOUNT_8', 'CURRENT_BALANCE_8', 'PRE_APPLY_TIME_28', 'PRE_EXPIRE_TIME_28', 'CUR_EXPIRE_TIME_28', 'CURRENCY_ID_9', 'OPER_TYPE_28', 'ACCT_ID_20', 'ACCT_BALANCE_ID_19', 'BALANCE_TYPE_19', 'BONUS_AMOUNT_9', 'CURRENT_BALANCE_9', 'PRE_APPLY_TIME_29', 'PRE_EXPIRE_TIME_29', 'CUR_EXPIRE_TIME_29', 'CURRENCY_ID_10', 'OPER_TYPE_29', 'FU_OWN_TYPE_10', 'FU_OWN_ID_10', 'FREE_UNIT_TYPE_10', 'FREE_UNIT_ID_10', 'BONUS_AMOUNT_10', 'CURRENT_AMOUNT', 'PRE_APPLY_TIME_30', 'PRE_EXPIRE_TIME_30', 'CUR_EXPIRE_TIME_30', 'FU_MEASURE_ID_10', 'OPER_TYPE_30', 'FU_OWN_TYPE_11', 'FU_OWN_ID_11', 'FREE_UNIT_TYPE_11', 'FREE_UNIT_ID_11', 'BONUS_AMOUNT_11', 'CURRENT_AMOUNT_1', 'PRE_APPLY_TIME_31', 'PRE_EXPIRE_TIME_31', 'CUR_EXPIRE_TIME_31', 'FU_MEASURE_ID_11', 'OPER_TYPE_31', 'FU_OWN_TYPE_12', 'FU_OWN_ID_12', 'FREE_UNIT_TYPE_12', 'FREE_UNIT_ID_12', 'BONUS_AMOUNT_12', 'CURRENT_AMOUNT_2', 'PRE_APPLY_TIME_32', 'PRE_EXPIRE_TIME_32', 'CUR_EXPIRE_TIME_32', 'FU_MEASURE_ID_12', 'OPER_TYPE_32', 'FU_OWN_TYPE_13', 'FU_OWN_ID_13', 'FREE_UNIT_TYPE_13', 'FREE_UNIT_ID_13', 'BONUS_AMOUNT_13', 'CURRENT_AMOUNT_3', 'PRE_APPLY_TIME_33', 'PRE_EXPIRE_TIME_33', 'CUR_EXPIRE_TIME_33', 'FU_MEASURE_ID_13', 'OPER_TYPE_33', 'FU_OWN_TYPE_14', 'FU_OWN_ID_14', 'FREE_UNIT_TYPE_14', 'FREE_UNIT_ID_14', 'BONUS_AMOUNT_14', 'CURRENT_AMOUNT_4', 'PRE_APPLY_TIME_34', 'PRE_EXPIRE_TIME_34', 'CUR_EXPIRE_TIME_34', 'FU_MEASURE_ID_14', 'OPER_TYPE_34', 'FU_OWN_TYPE_15', 'FU_OWN_ID_15', 'FREE_UNIT_TYPE_15', 'FREE_UNIT_ID_15', 'BONUS_AMOUNT_15', 'CURRENT_AMOUNT_5', 'PRE_APPLY_TIME_35', 'PRE_EXPIRE_TIME_35', 'CUR_EXPIRE_TIME_35', 'FU_MEASURE_ID_15', 'OPER_TYPE_35', 'FU_OWN_TYPE_16', 'FU_OWN_ID_16', 'FREE_UNIT_TYPE_16', 'FREE_UNIT_ID_16', 'BONUS_AMOUNT_16', 'CURRENT_AMOUNT_6', 'PRE_APPLY_TIME_36', 'PRE_EXPIRE_TIME_36', 'CUR_EXPIRE_TIME_36', 'FU_MEASURE_ID_16', 'OPER_TYPE_36', 'FU_OWN_TYPE_17', 'FU_OWN_ID_17', 'FREE_UNIT_TYPE_17', 'FREE_UNIT_ID_17', 'BONUS_AMOUNT_17', 'CURRENT_AMOUNT_7', 'PRE_APPLY_TIME_37', 'PRE_EXPIRE_TIME_37', 'CUR_EXPIRE_TIME_37', 'FU_MEASURE_ID_17', 'OPER_TYPE_37', 'FU_OWN_TYPE_18', 'FU_OWN_ID_18', 'FREE_UNIT_TYPE_18', 'FREE_UNIT_ID_18', 'BONUS_AMOUNT_18', 'CURRENT_AMOUNT_8', 'PRE_APPLY_TIME_38', 'PRE_EXPIRE_TIME_38', 'CUR_EXPIRE_TIME_38', 'FU_MEASURE_ID_18', 'OPER_TYPE_38', 'FU_OWN_TYPE_19', 'FU_OWN_ID_19', 'FREE_UNIT_TYPE_19', 'FREE_UNIT_ID_19', 'BONUS_AMOUNT_19', 'CURRENT_AMOUNT_9', 'PRE_APPLY_TIME_39', 'PRE_EXPIRE_TIME_39', 'CUR_EXPIRE_TIME_39', 'FU_MEASURE_ID_19', 'OPER_TYPE_39', 'RechargeAreaCode', 'RechargeCellID', 'BrandID', 'MainOfferingID', 'PayType', 'StartTimeOfBillCycle', 'Account', 'MainBalanceInfo', 'ChgBalanceInfo', 'ChgFreeUnitInfo', 'UserState', 'OldUserState', 'CardValueAdded', 'ValidityAdded', 'TradeType', 'AgentName', 'AdditionalInfo', 'BankCode', 'SubIdentityType', 'LoginSystemCode', 'LOAN_TRANS_ID','CUST_LOCAL_START_DATE']
    
    file_path ='/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/recharge/ltc_cdr_cbs_vou'
    columns_index = ['PRI_IDENTITY', 'ENTRY_DATE', 'StartTimeOfBillCycle']
    
    # loop data
    data_items = []
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')[:-1]
                
                for line in content_split:
                    if len(line.split('|')) > 3:
                        items = line.split('|')[:-1]
                        if len(data_items) < 100:
                            data_items.append(items)
                        else:
                            save_dataframe(data_items, item_columns, columns_index, file_path)
                            data_items = []
                            data_items.append(items)
    # save data
    if len(data_items) > 0:
        save_dataframe(data_items, item_columns, columns_index, file_path)


# transfer data
def Transfer_data():
    data = filter_file('ltc_pps_cbs_cdr_transfer')
    item_columns = ['TRANSFER_LOG_ID', 'PAY_TYPE', 'ACCT_ID', 'CUST_ID', 'SUB_ID', 'PRI_IDENTITY', 'BATCH_NO', 'CHANNEL_ID', 'REASON_CODE', 'RESULT_CODE', 'ERROR_TYPE', 'REQUEST_ACCT_BALANCE_ID', 'DEST_ACCT_ID', 'DEST_ACCT_BALANCE_ID', 'TRANSFER_TYPE', 'TRANSFER_AMT', 'TRANSFER_DATE', 'TRANSFER_TRANS_ID', 'EXT_TRANS_TYPE', 'EXT_TRANS_ID', 'ACCESS_METHOD', 'DIAMETER_SESSIONID', 'HANDLING_CHARGE', 'REVERSAL_TRANS_ID', 'REVERSAL_REASON_CODE', 'REVERSAL_DATE', 'EXT_REF_ID', 'STATUS', 'ENTRY_DATE', 'REMARK', 'OPER_ID', 'DEPT_ID', 'BE_ID', 'BE_CODE', 'REGION_ID', 'REGION_CODE', 'ACCT_ID_1', 'ACCT_BALANCE_ID', 'BALANCE_TYPE', 'CUR_BALANCE', 'CHG_BALANCE', 'PRE_APPLY_TIME', 'PRE_EXPIRE_TIME', 'CUR_EXPIRE_TIME', 'CURRENCYE_ID', 'OPER_TYPE', 'ACCT_ID_2', 'ACCT_BALANCE_ID_1', 'BALANCE_TYPE_1', 'CUR_BALANCE_1', 'CHG_BALANCE_1', 'PRE_APPLY_TIME_1', 'PRE_EXPIRE_TIME_1', 'CUR_EXPIRE_TIME_1', 'CURRENCYE_ID_1', 'OPER_TYPE_1', 'ACCT_ID_3', 'ACCT_BALANCE_ID_2', 'BALANCE_TYPE_2', 'CUR_BALANCE_2', 'CHG_BALANCE_2', 'PRE_APPLY_TIME_2', 'PRE_EXPIRE_TIME_2', 'CUR_EXPIRE_TIME_2', 'CURRENCYE_ID_2', 'OPER_TYPE_2', 'ACCT_ID_4', 'ACCT_BALANCE_ID_3', 'BALANCE_TYPE_3', 'CUR_BALANCE_3', 'CHG_BALANCE_3', 'PRE_APPLY_TIME_3', 'PRE_EXPIRE_TIME_3', 'CUR_EXPIRE_TIME_3', 'CURRENCYE_ID_3', 'OPER_TYPE_3', 'ACCT_ID_5', 'ACCT_BALANCE_ID_4', 'BALANCE_TYPE_4', 'CUR_BALANCE_4', 'CHG_BALANCE_4', 'PRE_APPLY_TIME_4', 'PRE_EXPIRE_TIME_4', 'CUR_EXPIRE_TIME_4', 'CURRENCYE_ID_4', 'OPER_TYPE_4', 'ACCT_ID_6', 'ACCT_BALANCE_ID_5', 'BALANCE_TYPE_5', 'CUR_BALANCE_5', 'CHG_BALANCE_5', 'PRE_APPLY_TIME_5', 'PRE_EXPIRE_TIME_5', 'CUR_EXPIRE_TIME_5', 'CURRENCYE_ID_5', 'OPER_TYPE_5', 'ACCT_ID_7', 'ACCT_BALANCE_ID_6', 'BALANCE_TYPE_6', 'CUR_BALANCE_6', 'CHG_BALANCE_6', 'PRE_APPLY_TIME_6', 'PRE_EXPIRE_TIME_6', 'CUR_EXPIRE_TIME_6', 'CURRENCYE_ID_6', 'OPER_TYPE_6', 'ACCT_ID_8', 'ACCT_BALANCE_ID_7', 'BALANCE_TYPE_7', 'CUR_BALANCE_7', 'CHG_BALANCE_7', 'PRE_APPLY_TIME_7', 'PRE_EXPIRE_TIME_7', 'CUR_EXPIRE_TIME_7', 'CURRENCYE_ID_7', 'OPER_TYPE_7', 'ACCT_ID_9', 'ACCT_BALANCE_ID_8', 'BALANCE_TYPE_8', 'CUR_BALANCE_8', 'CHG_BALANCE_8', 'PRE_APPLY_TIME_8', 'PRE_EXPIRE_TIME_8', 'CUR_EXPIRE_TIME_8', 'CURRENCYE_ID_8', 'OPER_TYPE_8', 'ACCT_ID_10', 'ACCT_BALANCE_ID_9', 'BALANCE_TYPE_9', 'CUR_BALANCE_9', 'CHG_BALANCE_9', 'PRE_APPLY_TIME_9', 'PRE_EXPIRE_TIME_9', 'CUR_EXPIRE_TIME_9', 'CURRENCYE_ID_9', 'OPER_TYPE_9', 'FU_OWN_TYPE', 'FU_OWN_ID', 'FREE_UNIT_ID ', 'FREE_UNIT_TYPE ', 'CUR_AMOUNT ', 'CHG_AMOUNT ', 'PRE_APPLY_TIME_10', 'PRE_EXPIRE_TIME_10', 'CUR_EXPIRE_TIME_10', 'FU_MEASURE_ID ', 'OPER_TYPE_10', 'FU_OWN_TYPE_1', 'FU_OWN_ID_1', 'FREE_UNIT_ID _1', 'FREE_UNIT_TYPE _1', 'CUR_AMOUNT _1', 'CHG_AMOUNT _1', 'PRE_APPLY_TIME_11', 'PRE_EXPIRE_TIME_11', 'CUR_EXPIRE_TIME_11', 'FU_MEASURE_ID _1', 'OPER_TYPE_11', 'FU_OWN_TYPE_2', 'FU_OWN_ID_2', 'FREE_UNIT_ID _2', 'FREE_UNIT_TYPE _2', 'CUR_AMOUNT _2', 'CHG_AMOUNT _2', 'PRE_APPLY_TIME_12', 'PRE_EXPIRE_TIME_12', 'CUR_EXPIRE_TIME_12', 'FU_MEASURE_ID _2', 'OPER_TYPE_12', 'FU_OWN_TYPE_3', 'FU_OWN_ID_3', 'FREE_UNIT_ID _3', 'FREE_UNIT_TYPE _3', 'CUR_AMOUNT _3', 'CHG_AMOUNT _3', 'PRE_APPLY_TIME_13', 'PRE_EXPIRE_TIME_13', 'CUR_EXPIRE_TIME_13', 'FU_MEASURE_ID _3', 'OPER_TYPE_13', 'FU_OWN_TYPE_4', 'FU_OWN_ID_4', 'FREE_UNIT_ID _4', 'FREE_UNIT_TYPE _4', 'CUR_AMOUNT _4', 'CHG_AMOUNT _4', 'PRE_APPLY_TIME_14', 'PRE_EXPIRE_TIME_14', 'CUR_EXPIRE_TIME_14', 'FU_MEASURE_ID _4', 'OPER_TYPE_14', 'FU_OWN_TYPE_5', 'FU_OWN_ID_5', 'FREE_UNIT_ID _5', 'FREE_UNIT_TYPE _5', 'CUR_AMOUNT _5', 'CHG_AMOUNT _5', 'PRE_APPLY_TIME_15', 'PRE_EXPIRE_TIME_15', 'CUR_EXPIRE_TIME_15', 'FU_MEASURE_ID _5', 'OPER_TYPE_15', 'FU_OWN_TYPE_6', 'FU_OWN_ID_6', 'FREE_UNIT_ID _6', 'FREE_UNIT_TYPE _6', 'CUR_AMOUNT _6', 'CHG_AMOUNT _6', 'PRE_APPLY_TIME_16', 'PRE_EXPIRE_TIME_16', 'CUR_EXPIRE_TIME_16', 'FU_MEASURE_ID _6', 'OPER_TYPE_16', 'FU_OWN_TYPE_7', 'FU_OWN_ID_7', 'FREE_UNIT_ID _7', 'FREE_UNIT_TYPE _7', 'CUR_AMOUNT _7', 'CHG_AMOUNT _7', 'PRE_APPLY_TIME_17', 'PRE_EXPIRE_TIME_17', 'CUR_EXPIRE_TIME_17', 'FU_MEASURE_ID _7', 'OPER_TYPE_17', 'FU_OWN_TYPE_8', 'FU_OWN_ID_8', 'FREE_UNIT_ID _8', 'FREE_UNIT_TYPE _8', 'CUR_AMOUNT _8', 'CHG_AMOUNT _8', 'PRE_APPLY_TIME_18', 'PRE_EXPIRE_TIME_18', 'CUR_EXPIRE_TIME_18', 'FU_MEASURE_ID _8', 'OPER_TYPE_18', 'FU_OWN_TYPE_9', 'FU_OWN_ID_9', 'FREE_UNIT_ID _9', 'FREE_UNIT_TYPE _9', 'CUR_AMOUNT _9', 'CHG_AMOUNT _9', 'PRE_APPLY_TIME_19', 'PRE_EXPIRE_TIME_19', 'CUR_EXPIRE_TIME_19', 'FU_MEASURE_ID _9', 'OPER_TYPE_19', 'ACCT_ID ', 'ACCT_BALANCE_ID ', 'BALANCE_TYPE ', 'BONUS_AMOUNT ', 'CURRENT_BALANCE', 'PRE_APPLY_TIME ', 'PRE_EXPIRE_TIME ', 'CUR_EXPIRE_TIME ', 'CURRENCY_ID', 'OPER_TYPE_20', 'ACCT_ID _1', 'ACCT_BALANCE_ID _1', 'BALANCE_TYPE _1', 'BONUS_AMOUNT _1', 'CURRENT_BALANCE_1', 'PRE_APPLY_TIME _1', 'PRE_EXPIRE_TIME _1', 'CUR_EXPIRE_TIME _1', 'CURRENCY_ID_1', 'OPER_TYPE_21', 'ACCT_ID _2', 'ACCT_BALANCE_ID _2', 'BALANCE_TYPE _2', 'BONUS_AMOUNT _2', 'CURRENT_BALANCE_2', 'PRE_APPLY_TIME _2', 'PRE_EXPIRE_TIME _2', 'CUR_EXPIRE_TIME _2', 'CURRENCY_ID_2', 'OPER_TYPE_22', 'ACCT_ID _3', 'ACCT_BALANCE_ID _3', 'BALANCE_TYPE _3', 'BONUS_AMOUNT _3', 'CURRENT_BALANCE_3', 'PRE_APPLY_TIME _3', 'PRE_EXPIRE_TIME _3', 'CUR_EXPIRE_TIME _3', 'CURRENCY_ID_3', 'OPER_TYPE_23', 'ACCT_ID _4', 'ACCT_BALANCE_ID _4', 'BALANCE_TYPE _4', 'BONUS_AMOUNT _4', 'CURRENT_BALANCE_4', 'PRE_APPLY_TIME _4', 'PRE_EXPIRE_TIME _4', 'CUR_EXPIRE_TIME _4', 'CURRENCY_ID_4', 'OPER_TYPE_24', 'ACCT_ID _5', 'ACCT_BALANCE_ID _5', 'BALANCE_TYPE _5', 'BONUS_AMOUNT _5', 'CURRENT_BALANCE_5', 'PRE_APPLY_TIME _5', 'PRE_EXPIRE_TIME _5', 'CUR_EXPIRE_TIME _5', 'CURRENCY_ID_5', 'OPER_TYPE_25', 'ACCT_ID _6', 'ACCT_BALANCE_ID _6', 'BALANCE_TYPE _6', 'BONUS_AMOUNT _6', 'CURRENT_BALANCE_6', 'PRE_APPLY_TIME _6', 'PRE_EXPIRE_TIME _6', 'CUR_EXPIRE_TIME _6', 'CURRENCY_ID_6', 'OPER_TYPE_26', 'ACCT_ID _7', 'ACCT_BALANCE_ID _7', 'BALANCE_TYPE _7', 'BONUS_AMOUNT _7', 'CURRENT_BALANCE_7', 'PRE_APPLY_TIME _7', 'PRE_EXPIRE_TIME _7', 'CUR_EXPIRE_TIME _7', 'CURRENCY_ID_7', 'OPER_TYPE_27', 'ACCT_ID _8', 'ACCT_BALANCE_ID _8', 'BALANCE_TYPE _8', 'BONUS_AMOUNT _8', 'CURRENT_BALANCE_8', 'PRE_APPLY_TIME _8', 'PRE_EXPIRE_TIME _8', 'CUR_EXPIRE_TIME _8', 'CURRENCY_ID_8', 'OPER_TYPE_28', 'ACCT_ID _9', 'ACCT_BALANCE_ID _9', 'BALANCE_TYPE _9', 'BONUS_AMOUNT _9', 'CURRENT_BALANCE_9', 'PRE_APPLY_TIME _9', 'PRE_EXPIRE_TIME _9', 'CUR_EXPIRE_TIME _9', 'CURRENCY_ID_9', 'OPER_TYPE_29', 'FU_OWN_TYPE ', 'FU_OWN_ID ', 'FREE_UNIT_TYPE _10', 'FREE_UNIT_ID', 'BONUS_AMOUNT', 'CURRENT_AMOUNT', 'PRE_APPLY_TIME_20', 'PRE_EXPIRE_TIME_20', 'CUR_EXPIRE_TIME_20', 'FU_MEASURE_ID', 'OPER_TYPE_30', 'FU_OWN_TYPE _1', 'FU_OWN_ID _1', 'FREE_UNIT_TYPE _11', 'FREE_UNIT_ID_1', 'BONUS_AMOUNT_1', 'CURRENT_AMOUNT_1', 'PRE_APPLY_TIME_21', 'PRE_EXPIRE_TIME_21', 'CUR_EXPIRE_TIME_21', 'FU_MEASURE_ID_1', 'OPER_TYPE_31', 'FU_OWN_TYPE _2', 'FU_OWN_ID _2', 'FREE_UNIT_TYPE _12', 'FREE_UNIT_ID_2', 'BONUS_AMOUNT_2', 'CURRENT_AMOUNT_2', 'PRE_APPLY_TIME_22', 'PRE_EXPIRE_TIME_22', 'CUR_EXPIRE_TIME_22', 'FU_MEASURE_ID_2', 'OPER_TYPE_32', 'FU_OWN_TYPE _3', 'FU_OWN_ID _3', 'FREE_UNIT_TYPE _13', 'FREE_UNIT_ID_3', 'BONUS_AMOUNT_3', 'CURRENT_AMOUNT_3', 'PRE_APPLY_TIME_23', 'PRE_EXPIRE_TIME_23', 'CUR_EXPIRE_TIME_23', 'FU_MEASURE_ID_3', 'OPER_TYPE_33', 'FU_OWN_TYPE _4', 'FU_OWN_ID _4', 'FREE_UNIT_TYPE _14', 'FREE_UNIT_ID_4', 'BONUS_AMOUNT_4', 'CURRENT_AMOUNT_4', 'PRE_APPLY_TIME_24', 'PRE_EXPIRE_TIME_24', 'CUR_EXPIRE_TIME_24', 'FU_MEASURE_ID_4', 'OPER_TYPE_34', 'FU_OWN_TYPE _5', 'FU_OWN_ID _5', 'FREE_UNIT_TYPE _15', 'FREE_UNIT_ID_5', 'BONUS_AMOUNT_5', 'CURRENT_AMOUNT_5', 'PRE_APPLY_TIME_25', 'PRE_EXPIRE_TIME_25', 'CUR_EXPIRE_TIME_25', 'FU_MEASURE_ID_5', 'OPER_TYPE_35', 'FU_OWN_TYPE _6', 'FU_OWN_ID _6', 'FREE_UNIT_TYPE _16', 'FREE_UNIT_ID_6', 'BONUS_AMOUNT_6', 'CURRENT_AMOUNT_6', 'PRE_APPLY_TIME_26', 'PRE_EXPIRE_TIME_26', 'CUR_EXPIRE_TIME_26', 'FU_MEASURE_ID_6', 'OPER_TYPE_36', 'FU_OWN_TYPE _7', 'FU_OWN_ID _7', 'FREE_UNIT_TYPE _17', 'FREE_UNIT_ID_7', 'BONUS_AMOUNT_7', 'CURRENT_AMOUNT_7', 'PRE_APPLY_TIME_27', 'PRE_EXPIRE_TIME_27', 'CUR_EXPIRE_TIME_27', 'FU_MEASURE_ID_7', 'OPER_TYPE_37', 'FU_OWN_TYPE _8', 'FU_OWN_ID _8', 'FREE_UNIT_TYPE _18', 'FREE_UNIT_ID_8', 'BONUS_AMOUNT_8', 'CURRENT_AMOUNT_8', 'PRE_APPLY_TIME_28', 'PRE_EXPIRE_TIME_28', 'CUR_EXPIRE_TIME_28', 'FU_MEASURE_ID_8', 'OPER_TYPE_38', 'FU_OWN_TYPE _9', 'FU_OWN_ID _9', 'FREE_UNIT_TYPE _19', 'FREE_UNIT_ID_9', 'BONUS_AMOUNT_9', 'CURRENT_AMOUNT_9', 'PRE_APPLY_TIME_29', 'PRE_EXPIRE_TIME_29', 'CUR_EXPIRE_TIME_29', 'FU_MEASURE_ID_9', 'OPER_TYPE_39', 'BrandID', 'MainOfferingID', 'PayType', 'StartTimeOfBillCycle', 'DETAIL_FEE', 'Account', 'MainBalanceInfo', 'ChgBalanceInfo', 'ChgFreeUnitInfo', 'UserState', 'oldUserState', 'TransferType', 'Dest_Pri_Identity', 'SPID', 'AdditionalInfo', 'Merchant', 'Service', 'LOAN_TRANS_ID', 'ACCOUNT_KEY', 'OWNER_CUST_CODE', 'SUBSCRIBER_KEY', 'CUST_LOCAL_START_DATE','RECIPIENT_NUMBER']
    
    file_path ='/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/transfer/ltc_cdr_cbs_transfer'
    columns_index = ['PRI_IDENTITY', 'TRANSFER_DATE', 'ENTRY_DATE', 'SUBSCRIBER_KEY', 'StartTimeOfBillCycle']

    
    # loop data
    data_items = []
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')
                
                for line in content_split:
                    if len(line.split('|')) > 3:
                        items = line.split('|')[:-1]
                        if len(data_items) < 100:
                            data_items.append(items)
                        else:
                            save_dataframe(data_items, item_columns, columns_index, file_path)
                            data_items = []
                            data_items.append(items)
    # save data
    if len(data_items) > 0:
        save_dataframe(data_items, item_columns, columns_index, file_path)

# Adjustment data
def Adjustment_data():
    data = filter_file('ltc_pps_cbs_cdr_adj')
    item_columns = ['ADJUST_LOG_ID', 'ACCT_ID', 'CUST_ID', 'SUB_ID', 'PRI_IDENTITY', 'PAY_TYPE', 'BATCH_NO', 'CHANNEL_ID', 'REASON_CODE', 'RESULT_CODE', 'ERROR_TYPE', 'ACCT_BALANCE_ID', 'ADJUST_AMT', 'ADJUST_TRANS_ID', 'EXT_TRANS_TYPE', 'EXT_TRANS_ID', 'ACCESS_METHOD', 'REVERSAL_TRANS_ID', 'REVERSAL_REASON_CODE', 'REVERSAL_DATE', 'STATUS', 'ENTRY_DATE', 'OPER_ID', 'DEPT_ID', 'REMARK', 'BE_ID', 'BE_CODE', 'REGION_ID', 'REGION_CODE', 'DEBIT_AMOUNT', 'Reserved', 'DEBIT_FROM_PREPAID', 'DEBIT_FROM_ADVANCE_PREPAID', 'DEBIT_FROM_POSTPAID', 'DEBIT_FROM_ADVANCE_POSTPAID', 'DEBIT_FROM_CREDIT_POSTPAID', 'TOTAL_TAX', 'FREE_UNIT_AMOUNT_OF_TIMES', 'FREE_UNIT_AMOUNT_OF_DURATION ', 'FREE_UNIT_AMOUNT_OF_FLUX ', 'ACCT_ID_1', 'ACCT_BALANCE_ID_1', 'BALANCE_TYPE', 'CUR_BALANCE', 'CHG_BALANCE', 'PRE_APPLY_TIME', 'PRE_EXPIRE_TIME', 'CUR_EXPIRE_TIME', 'CURRENCYE_ID', 'OPER_TYPE', 'ACCT_ID_2', 'ACCT_BALANCE_ID_2', 'BALANCE_TYPE_1', 'CUR_BALANCE_1', 'CHG_BALANCE_1', 'PRE_APPLY_TIME_1', 'PRE_EXPIRE_TIME_1', 'CUR_EXPIRE_TIME_1', 'CURRENCYE_ID_1', 'OPER_TYPE_1', 'ACCT_ID_3', 'ACCT_BALANCE_ID_3', 'BALANCE_TYPE_2', 'CUR_BALANCE_2', 'CHG_BALANCE_2', 'PRE_APPLY_TIME_2', 'PRE_EXPIRE_TIME_2', 'CUR_EXPIRE_TIME_2', 'CURRENCYE_ID_2', 'OPER_TYPE_2', 'ACCT_ID_4', 'ACCT_BALANCE_ID_4', 'BALANCE_TYPE_3', 'CUR_BALANCE_3', 'CHG_BALANCE_3', 'PRE_APPLY_TIME_3', 'PRE_EXPIRE_TIME_3', 'CUR_EXPIRE_TIME_3', 'CURRENCYE_ID_3', 'OPER_TYPE_3', 'ACCT_ID_5', 'ACCT_BALANCE_ID_5', 'BALANCE_TYPE_4', 'CUR_BALANCE_4', 'CHG_BALANCE_4', 'PRE_APPLY_TIME_4', 'PRE_EXPIRE_TIME_4', 'CUR_EXPIRE_TIME_4', 'CURRENCYE_ID_4', 'OPER_TYPE_4', 'ACCT_ID_6', 'ACCT_BALANCE_ID_6', 'BALANCE_TYPE_5', 'CUR_BALANCE_5', 'CHG_BALANCE_5', 'PRE_APPLY_TIME_5', 'PRE_EXPIRE_TIME_5', 'CUR_EXPIRE_TIME_5', 'CURRENCYE_ID_5', 'OPER_TYPE_5', 'ACCT_ID_7', 'ACCT_BALANCE_ID_7', 'BALANCE_TYPE_6', 'CUR_BALANCE_6', 'CHG_BALANCE_6', 'PRE_APPLY_TIME_6', 'PRE_EXPIRE_TIME_6', 'CUR_EXPIRE_TIME_6', 'CURRENCYE_ID_6', 'OPER_TYPE_6', 'ACCT_ID_8', 'ACCT_BALANCE_ID_8', 'BALANCE_TYPE_7', 'CUR_BALANCE_7', 'CHG_BALANCE_7', 'PRE_APPLY_TIME_7', 'PRE_EXPIRE_TIME_7', 'CUR_EXPIRE_TIME_7', 'CURRENCYE_ID_7', 'OPER_TYPE_7', 'ACCT_ID_9', 'ACCT_BALANCE_ID_9', 'BALANCE_TYPE_8', 'CUR_BALANCE_8', 'CHG_BALANCE_8', 'PRE_APPLY_TIME_8', 'PRE_EXPIRE_TIME_8', 'CUR_EXPIRE_TIME_8', 'CURRENCYE_ID_8', 'OPER_TYPE_8', 'ACCT_ID_10', 'ACCT_BALANCE_ID_10', 'BALANCE_TYPE_9', 'CUR_BALANCE_9', 'CHG_BALANCE_9', 'PRE_APPLY_TIME_9', 'PRE_EXPIRE_TIME_9', 'CUR_EXPIRE_TIME_9', 'CURRENCYE_ID_9', 'OPER_TYPE_9', 'FU_OWN_TYPE', 'FU_OWN_ID', 'FREE_UNIT_ID ', 'FREE_UNIT_TYPE ', 'CUR_AMOUNT ', 'CHG_AMOUNT ', 'PRE_APPLY_TIME_10', 'PRE_EXPIRE_TIME_10', 'CUR_EXPIRE_TIME_10', 'FU_MEASURE_ID ', 'OPER_TYPE_10', 'FU_OWN_TYPE_1', 'FU_OWN_ID_1', 'FREE_UNIT_ID _1', 'FREE_UNIT_TYPE _1', 'CUR_AMOUNT _1', 'CHG_AMOUNT _1', 'PRE_APPLY_TIME_11', 'PRE_EXPIRE_TIME_11', 'CUR_EXPIRE_TIME_11', 'FU_MEASURE_ID _1', 'OPER_TYPE_11', 'FU_OWN_TYPE_2', 'FU_OWN_ID_2', 'FREE_UNIT_ID _2', 'FREE_UNIT_TYPE _2', 'CUR_AMOUNT _2', 'CHG_AMOUNT _2', 'PRE_APPLY_TIME_12', 'PRE_EXPIRE_TIME_12', 'CUR_EXPIRE_TIME_12', 'FU_MEASURE_ID _2', 'OPER_TYPE_12', 'FU_OWN_TYPE_3', 'FU_OWN_ID_3', 'FREE_UNIT_ID _3', 'FREE_UNIT_TYPE _3', 'CUR_AMOUNT _3', 'CHG_AMOUNT _3', 'PRE_APPLY_TIME_13', 'PRE_EXPIRE_TIME_13', 'CUR_EXPIRE_TIME_13', 'FU_MEASURE_ID _3', 'OPER_TYPE_13', 'FU_OWN_TYPE_4', 'FU_OWN_ID_4', 'FREE_UNIT_ID _4', 'FREE_UNIT_TYPE _4', 'CUR_AMOUNT _4', 'CHG_AMOUNT _4', 'PRE_APPLY_TIME_14', 'PRE_EXPIRE_TIME_14', 'CUR_EXPIRE_TIME_14', 'FU_MEASURE_ID _4', 'OPER_TYPE_14', 'FU_OWN_TYPE_5', 'FU_OWN_ID_5', 'FREE_UNIT_ID _5', 'FREE_UNIT_TYPE _5', 'CUR_AMOUNT _5', 'CHG_AMOUNT _5', 'PRE_APPLY_TIME_15', 'PRE_EXPIRE_TIME_15', 'CUR_EXPIRE_TIME_15', 'FU_MEASURE_ID _5', 'OPER_TYPE_15', 'FU_OWN_TYPE_6', 'FU_OWN_ID_6', 'FREE_UNIT_ID _6', 'FREE_UNIT_TYPE _6', 'CUR_AMOUNT _6', 'CHG_AMOUNT _6', 'PRE_APPLY_TIME_16', 'PRE_EXPIRE_TIME_16', 'CUR_EXPIRE_TIME_16', 'FU_MEASURE_ID _6', 'OPER_TYPE_16', 'FU_OWN_TYPE_7', 'FU_OWN_ID_7', 'FREE_UNIT_ID _7', 'FREE_UNIT_TYPE _7', 'CUR_AMOUNT _7', 'CHG_AMOUNT _7', 'PRE_APPLY_TIME_17', 'PRE_EXPIRE_TIME_17', 'CUR_EXPIRE_TIME_17', 'FU_MEASURE_ID _7', 'OPER_TYPE_17', 'FU_OWN_TYPE_8', 'FU_OWN_ID_8', 'FREE_UNIT_ID _8', 'FREE_UNIT_TYPE _8', 'CUR_AMOUNT _8', 'CHG_AMOUNT _8', 'PRE_APPLY_TIME_18', 'PRE_EXPIRE_TIME_18', 'CUR_EXPIRE_TIME_18', 'FU_MEASURE_ID _8', 'OPER_TYPE_18', 'FU_OWN_TYPE_9', 'FU_OWN_ID_9', 'FREE_UNIT_ID _9', 'FREE_UNIT_TYPE _9', 'CUR_AMOUNT _9', 'CHG_AMOUNT _9', 'PRE_APPLY_TIME_19', 'PRE_EXPIRE_TIME_19', 'CUR_EXPIRE_TIME_19', 'FU_MEASURE_ID _9', 'OPER_TYPE_19', 'ACCT_ID ', 'ACCT_BALANCE_ID ', 'BALANCE_TYPE ', 'BONUS_AMOUNT ', 'CURRENT_BALANCE', 'PRE_APPLY_TIME ', 'PRE_EXPIRE_TIME ', 'CUR_EXPIRE_TIME ', 'CURRENCY_ID', 'OPER_TYPE_20', 'ACCT_ID _1', 'ACCT_BALANCE_ID _1', 'BALANCE_TYPE _1', 'BONUS_AMOUNT _1', 'CURRENT_BALANCE_1', 'PRE_APPLY_TIME _1', 'PRE_EXPIRE_TIME _1', 'CUR_EXPIRE_TIME _1', 'CURRENCY_ID_1', 'OPER_TYPE_21', 'ACCT_ID _2', 'ACCT_BALANCE_ID _2', 'BALANCE_TYPE _2', 'BONUS_AMOUNT _2', 'CURRENT_BALANCE_2', 'PRE_APPLY_TIME _2', 'PRE_EXPIRE_TIME _2', 'CUR_EXPIRE_TIME _2', 'CURRENCY_ID_2', 'OPER_TYPE_22', 'ACCT_ID _3', 'ACCT_BALANCE_ID _3', 'BALANCE_TYPE _3', 'BONUS_AMOUNT _3', 'CURRENT_BALANCE_3', 'PRE_APPLY_TIME _3', 'PRE_EXPIRE_TIME _3', 'CUR_EXPIRE_TIME _3', 'CURRENCY_ID_3', 'OPER_TYPE_23', 'ACCT_ID _4', 'ACCT_BALANCE_ID _4', 'BALANCE_TYPE _4', 'BONUS_AMOUNT _4', 'CURRENT_BALANCE_4', 'PRE_APPLY_TIME _4', 'PRE_EXPIRE_TIME _4', 'CUR_EXPIRE_TIME _4', 'CURRENCY_ID_4', 'OPER_TYPE_24', 'ACCT_ID _5', 'ACCT_BALANCE_ID _5', 'BALANCE_TYPE _5', 'BONUS_AMOUNT _5', 'CURRENT_BALANCE_5', 'PRE_APPLY_TIME _5', 'PRE_EXPIRE_TIME _5', 'CUR_EXPIRE_TIME _5', 'CURRENCY_ID_5', 'OPER_TYPE_25', 'ACCT_ID _6', 'ACCT_BALANCE_ID _6', 'BALANCE_TYPE _6', 'BONUS_AMOUNT _6', 'CURRENT_BALANCE_6', 'PRE_APPLY_TIME _6', 'PRE_EXPIRE_TIME _6', 'CUR_EXPIRE_TIME _6', 'CURRENCY_ID_6', 'OPER_TYPE_26', 'ACCT_ID _7', 'ACCT_BALANCE_ID _7', 'BALANCE_TYPE _7', 'BONUS_AMOUNT _7', 'CURRENT_BALANCE_7', 'PRE_APPLY_TIME _7', 'PRE_EXPIRE_TIME _7', 'CUR_EXPIRE_TIME _7', 'CURRENCY_ID_7', 'OPER_TYPE_27', 'ACCT_ID _8', 'ACCT_BALANCE_ID _8', 'BALANCE_TYPE _8', 'BONUS_AMOUNT _8', 'CURRENT_BALANCE_8', 'PRE_APPLY_TIME _8', 'PRE_EXPIRE_TIME _8', 'CUR_EXPIRE_TIME _8', 'CURRENCY_ID_8', 'OPER_TYPE_28', 'ACCT_ID _9', 'ACCT_BALANCE_ID _9', 'BALANCE_TYPE _9', 'BONUS_AMOUNT _9', 'CURRENT_BALANCE_9', 'PRE_APPLY_TIME _9', 'PRE_EXPIRE_TIME _9', 'CUR_EXPIRE_TIME _9', 'CURRENCY_ID_9', 'OPER_TYPE_29', 'FU_OWN_TYPE ', 'FU_OWN_ID ', 'FREE_UNIT_TYPE _10', 'FREE_UNIT_ID', 'BONUS_AMOUNT', 'CURRENT_AMOUNT', 'PRE_APPLY_TIME_20', 'PRE_EXPIRE_TIME_20', 'CUR_EXPIRE_TIME_20', 'FU_MEASURE_ID', 'OPER_TYPE_30', 'FU_OWN_TYPE _1', 'FU_OWN_ID _1', 'FREE_UNIT_TYPE _11', 'FREE_UNIT_ID_1', 'BONUS_AMOUNT_1', 'CURRENT_AMOUNT_1', 'PRE_APPLY_TIME_21', 'PRE_EXPIRE_TIME_21', 'CUR_EXPIRE_TIME_21', 'FU_MEASURE_ID_1', 'OPER_TYPE_31', 'FU_OWN_TYPE _2', 'FU_OWN_ID _2', 'FREE_UNIT_TYPE _12', 'FREE_UNIT_ID_2', 'BONUS_AMOUNT_2', 'CURRENT_AMOUNT_2', 'PRE_APPLY_TIME_22', 'PRE_EXPIRE_TIME_22', 'CUR_EXPIRE_TIME_22', 'FU_MEASURE_ID_2', 'OPER_TYPE_32', 'FU_OWN_TYPE _3', 'FU_OWN_ID _3', 'FREE_UNIT_TYPE _13', 'FREE_UNIT_ID_3', 'BONUS_AMOUNT_3', 'CURRENT_AMOUNT_3', 'PRE_APPLY_TIME_23', 'PRE_EXPIRE_TIME_23', 'CUR_EXPIRE_TIME_23', 'FU_MEASURE_ID_3', 'OPER_TYPE_33', 'FU_OWN_TYPE _4', 'FU_OWN_ID _4', 'FREE_UNIT_TYPE _14', 'FREE_UNIT_ID_4', 'BONUS_AMOUNT_4', 'CURRENT_AMOUNT_4', 'PRE_APPLY_TIME_24', 'PRE_EXPIRE_TIME_24', 'CUR_EXPIRE_TIME_24', 'FU_MEASURE_ID_4', 'OPER_TYPE_34', 'FU_OWN_TYPE _5', 'FU_OWN_ID _5', 'FREE_UNIT_TYPE _15', 'FREE_UNIT_ID_5', 'BONUS_AMOUNT_5', 'CURRENT_AMOUNT_5', 'PRE_APPLY_TIME_25', 'PRE_EXPIRE_TIME_25', 'CUR_EXPIRE_TIME_25', 'FU_MEASURE_ID_5', 'OPER_TYPE_35', 'FU_OWN_TYPE _6', 'FU_OWN_ID _6', 'FREE_UNIT_TYPE _16', 'FREE_UNIT_ID_6', 'BONUS_AMOUNT_6', 'CURRENT_AMOUNT_6', 'PRE_APPLY_TIME_26', 'PRE_EXPIRE_TIME_26', 'CUR_EXPIRE_TIME_26', 'FU_MEASURE_ID_6', 'OPER_TYPE_36', 'FU_OWN_TYPE _7', 'FU_OWN_ID _7', 'FREE_UNIT_TYPE _17', 'FREE_UNIT_ID_7', 'BONUS_AMOUNT_7', 'CURRENT_AMOUNT_7', 'PRE_APPLY_TIME_27', 'PRE_EXPIRE_TIME_27', 'CUR_EXPIRE_TIME_27', 'FU_MEASURE_ID_7', 'OPER_TYPE_37', 'FU_OWN_TYPE _8', 'FU_OWN_ID _8', 'FREE_UNIT_TYPE _18', 'FREE_UNIT_ID_8', 'BONUS_AMOUNT_8', 'CURRENT_AMOUNT_8', 'PRE_APPLY_TIME_28', 'PRE_EXPIRE_TIME_28', 'CUR_EXPIRE_TIME_28', 'FU_MEASURE_ID_8', 'OPER_TYPE_38', 'FU_OWN_TYPE _9', 'FU_OWN_ID _9', 'FREE_UNIT_TYPE _19', 'FREE_UNIT_ID_9', 'BONUS_AMOUNT_9', 'CURRENT_AMOUNT_9', 'PRE_APPLY_TIME_29', 'PRE_EXPIRE_TIME_29', 'CUR_EXPIRE_TIME_29', 'FU_MEASURE_ID_9', 'OPER_TYPE_39', 'BrandID', 'MainOfferingID', 'PayType', 'StartTimeOfBillCycle', 'Account', 'MainBalanceInfo', 'ChgBalanceInfo', 'ChgFreeUnitInfo', 'UserState', 'oldUserState', 'SPID', 'AdditionalInfo', 'Merchant', 'Service', 'TAX_CODE_ID', 'TAX_AMOUNT', 'TAX_PRICE_FLAG', 'TAX_CODE_ID_1', 'TAX_AMOUNT_1', 'TAX_PRICE_FLAG_1', 'TAX_CODE_ID_2', 'TAX_AMOUNT_2', 'TAX_PRICE_FLAG_2', 'TAX_CODE_ID_3', 'TAX_AMOUNT_3', 'TAX_PRICE_FLAG_3', 'TAX_CODE_ID_4', 'TAX_AMOUNT_4', 'TAX_PRICE_FLAG_4', 'TAX_CODE_ID_5', 'TAX_AMOUNT_5', 'TAX_PRICE_FLAG_5', 'TAX_CODE_ID_6', 'TAX_AMOUNT_6', 'TAX_PRICE_FLAG_6', 'TAX_CODE_ID_7', 'TAX_AMOUNT_7', 'TAX_PRICE_FLAG_7', 'TAX_CODE_ID_8', 'TAX_AMOUNT_8', 'TAX_PRICE_FLAG_8', 'TAX_CODE_ID_9', 'TAX_AMOUNT_9', 'TAX_PRICE_FLAG_9', 'LoginSystemCode', 'OperType', 'LOAN_TRANS_ID', 'ACCOUNT_KEY', 'OWNER_CUST_CODE', 'SUBSCRIBER_KEY', 'LOAN_AMOUNT', 'LOAN_POUNDAGE','CUST_LOCAL_START_DATE']
    
    file_path ='/Users/isdd/Documents/Dev_Code/python_code/ELTBIGDATA/output/adjustment/ltc_cdr_cbs_adj'
    columns_index = ['PRI_IDENTITY', 'REVERSAL_DATE', 'ENTRY_DATE']
    
    # loop data
    data_items = []
    for file_name in data:
        if os.path.isfile(file_name):
            with open(file_name, 'r') as file:
                content = file.read()
                content_split = content.split('\n')[:-1]
                
                for line in content_split:
                    if len(line.split('|')) > 3:
                        items = line.split('|')[:-1]
                        if len(data_items) < 100:
                            data_items.append(items)
                        else:
                            save_dataframe(data_items, item_columns, columns_index, file_path)
                            data_items = []
                            data_items.append(items)
    # save data
    if len(data_items) > 0:
        save_dataframe(data_items, item_columns, columns_index, file_path)


# voice_data()
# sms_data()
# recurring_data()
# Management_data()
# Resource_clearance_data()
# first_time_activation_data()
# Recharge_data()
# Transfer_data()
# Adjustment_data()
# for file_name in os.listdir(folder_path):
    
#     print(file_name)
#     # file_path = os.path.join(folder_path, file_name)
#     # new_file_path = file_path.replace('.add', '.txt')
#     # os.rename(file_path, new_file_path)

    # # Find duplicates
    # duplicates = [text for text in set(concat_common_voice_columns) if concat_common_voice_columns.count(text) > 1]

    # print(duplicates)  # Output: ['apple', 'banana']








