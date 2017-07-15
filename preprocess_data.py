import pandas as pd
import os

datapath = "data/creditscoring"

with open(os.path.join(datapath, "german_credit.csv"), "r") as infile:
    raw = pd.read_csv(infile)

def replace_account_balance(x):
    mapper = {1: "< 0 DM",
              2: "0 <= 200 DM",
              3: ">= 200 DM / salary assignments for at least 1 year",
              4: "no checking account"}
    return mapper.get(x)
raw['Account Balance'] = raw['Account Balance'].map(replace_account_balance)

def replace_credit_history(x):
    mapper = {0: "no credits taken/ all credits paid back duly",
              1: "all credits at this bank paid back duly",
              2: "existing credits paid back duly till now",
              3: "delay in paying off in the past",
              4: "critical account/ other credits existing (not at this bank)"}
    return mapper.get(x)
raw['Payment Status of Previous Credit'] = raw['Payment Status of Previous Credit'].map(replace_credit_history)

def replace_purpose(x):
    mapper = {0: "car (new)",
              1: "car (used)",
              2: "furniture/equipment",
              3: "radio/television",
              4: "domestic appliances",
              5: "repairs",
              6: "education",
              7: "(vacation - does not exist?)",
              8: "retraining",
              9: "business",
              10: "others"}
    return mapper.get(x)
raw['Purpose'] = raw['Purpose'].map(replace_purpose)

def replace_savings(x):
    mapper = {1: "< 100 DM",
              2: "100 <= ... < 500 DM",
              3: "500 <= ... < 1000 DM",
              4: ">= 1000 DM",
              5: "unknown/ no savings account"}
    return mapper.get(x)
raw['Value Savings/Stocks'] = raw['Value Savings/Stocks'].map(replace_savings)

def replace_employment(x):
    mapper = {1: "unemployed",
              2: "< 1 year",
              3: "1 <= ... < 4 years ",
              4: "4 <= ... < 7 years",
              5: ">= 7 years"}
    return mapper.get(x)
raw['Length of current employment'] = raw['Length of current employment'].map(replace_employment)

def replace_personal_status(x):
    mapper = {1: "male : divorced/separated",
              2: "female : divorced/separated/married",
              3: "male : single",
              4: "male : married/widowed",
              5: "female : single"}
    return mapper.get(x)
raw['Sex & Marital Status'] = raw['Sex & Marital Status'].map(replace_personal_status)

def replace_guarantors(x):
    mapper = {1: "none",
              2: "co-applicant",
              3: "guarantor"}
    return mapper.get(x)
raw['Guarantors'] = raw['Guarantors'].map(replace_guarantors)

def replace_property(x):
    mapper = {1: "real estate",
              2: "if not real estate: building society savings agreement/ life insurance",
              3: "if not real estate/insurance : car or other, not in attribute Savings",
              4: "unknown / no property"}
    return mapper.get(x)
raw['Most valuable available asset'] = raw['Most valuable available asset'].map(replace_property)

def replace_other_credits(x):
    mapper = {1: "bank",
              2: "stores",
              3: "none"}
    return mapper.get(x)
    raw['Concurrent Credits'] = raw['Concurrent Credits'].map(replace_other_credits)

def replace_housing(x):
    mapper = {1: "rent",
              2: "own",
              3: "for free"}
    return mapper.get(x)
raw['Type of apartment'] = raw['Type of apartment'].map(replace_housing)

def replace_occupation(x):
    mapper = {1: "unemployed/ unskilled - non-resident",
              2: "unskilled - resident",
              3: "skilled employee / official",
              4: "management/self-employed/highly qualified employee/officer"}
    return mapper.get(x)
raw['Occupation'] = raw['Occupation'].map(replace_occupation)

def replace_telephone(x):
    mapper = {1: "none",
              2: "yes, registered under the customers name"}
    return mapper.get(x)
raw['Telephone'] = raw['Telephone'].map(replace_telephone)

def replace_foreign_status(x):
    mapper = {1: "yes",
              2: "no"}
    return mapper.get(x)
raw['Foreign Worker'] = raw['Foreign Worker'].map(replace_foreign_status)


with open(os.path.join(datapath, "german_credit_preprocessed.csv"), "w") as outfile:
    raw.to_csv(outfile, index=False)
