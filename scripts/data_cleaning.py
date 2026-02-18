import pandas as pd
import numpy as np

class DataCleaning:
    
    #Handeling missing values
    def handle_missing_values(self, df, strategy="mean"):
     if strategy == "mean":
        numeric_cols = df.select_dtypes(include=["number"]).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        return df

     elif strategy == "median":
        numeric_cols = df.select_dtypes(include=["number"]).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
        return df

     elif strategy == "mode":
        return df.fillna(df.mode().iloc[0])

     else:
        return df.dropna()


    #Remove duplicates from the DataFrame    
    def remove_duplicates(self, df):
        return df.drop_duplicates()
    
    #fix data types of columns into int
    def fix_data_types(self, df, column, new_type):
        df[column] = df[column].astype(int)
        return df
    
    #Remove outliers using the IQR method
    def remove_outliers(self, df, column):
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    
    #drop irrelevant columns from the DataFrame
    def drop_irrelevant_columns(self, df, columns):
        return df.drop(columns=columns)
    
    #convert categorical variables into dummy/indicator variables
    def encode_categorical_variables(self, df, column):
        return pd.get_dummies(df, columns=[column], drop_first=True)
    
    #clean the data by applying all the cleaning steps
    def clean_data(self, df, missing_value_strategy='mean', outlier_column=None, irrelevant_columns=None, categorical_column=None, data_type_fixes=None):
        df = self.handle_missing_values(df, strategy=missing_value_strategy)
        df = self.remove_duplicates(df)
        if outlier_column:
            df = self.remove_outliers(df, column=outlier_column)
        if irrelevant_columns:
            df = self.drop_irrelevant_columns(df, columns=irrelevant_columns)
        if categorical_column:
            df = self.encode_categorical_variables(df, column=categorical_column)
        if data_type_fixes:
            for column, new_type in data_type_fixes.items():
                df = self.fix_data_types(df, column, new_type)
        return df