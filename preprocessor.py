
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

class Preprocessing():

    def __init__(self, df: pd.DataFrame) -> None:
        '''
        Takes as input a dataframe, either test or train.
        '''    
        self.__dataframe__ = df
    

    def drop(self, columns_to_drop: list = []) -> pd.DataFrame:
        '''
        Takes as input the list of columns to drop and returns the dataframe.
        '''
        self.__dataframe__ = self.__dataframe__.drop(columns = columns_to_drop, axis = 1)

        return self.__dataframe__

    def discard_zeros(self, columns: str = '') -> pd.DataFrame:
        '''
        Takes as input the list of columns from which we want to discard the zeros.
        '''
        self.__dataframe__ = self.__dataframe__[self.__dataframe__[columns] != 0]

        return self.__dataframe__

    def discard_negatives(self, columns: str = '', include_zeros = False) -> pd.DataFrame:

        if include_zeros:
            self.__dataframe__ = self.__dataframe__[self.__dataframe__[columns] >= 0]
        else:
            self.__dataframe__ = self.__dataframe__[self.__dataframe__[columns] > 0]
    
    
    def fill_nan(self, imgs_mean = 0, videos_mean = 0 , key_mean = 0, columns: list = [], train = True) -> pd.DataFrame:
        '''
        Fill missing values with the mean.
        '''
        
        if train:

            self.__dataframe__[columns] = self.__dataframe__[columns].fillna(self.__dataframe__[columns].mean())
            return self.__dataframe__['num_imgs'].median(), self.__dataframe__['num_videos'].median(), self.__dataframe__['num_keywords'].median()

        else:
            self.__dataframe__['num_imgs'] = self.__dataframe__['num_imgs'].fillna(imgs_mean)
            self.__dataframe__['num_videos'] = self.__dataframe__['num_videos'].fillna(videos_mean)
            self.__dataframe__['num_keywords'] = self.__dataframe__['num_keywords'].fillna(key_mean)

        return self.__dataframe__
    
    def encode_weekdays(self) -> pd.DataFrame:
        '''
        Encode weekdays.
        '''
        self.__dataframe__['weekday'] = np.where(self.__dataframe__['weekday'].isin(['monday', 'thursday', 'wednesday', 'tuesday', 'friday']), 'Not Weekend', 'Weekend')

        return self.__dataframe__

    def make_combination(self, weights: dict, name_combination: str = "", drop: bool = True) -> pd.DataFrame:

        combination = sum(self.__dataframe__[column] * weight for column, weight in weights.items())
        self.__dataframe__[name_combination] = combination
        
        if drop:
            self.__dataframe__ = self.__dataframe__.drop(columns = weights.keys(), axis = 1)

        return self.__dataframe__
    
    def multiply_columns(self, columns: list = [], name_combination: str = "", drop: bool = True) -> pd.DataFrame:

        self.__dataframe__[name_combination] = self.__dataframe__[columns].prod(axis = 1)

        return self.__dataframe__
    
    def get_dataframe(self) -> pd.DataFrame:

        return self.__dataframe__
    
    def apply_one_hot(self, column: str = '') -> pd.DataFrame:
        
        one_hot_encoded = pd.get_dummies(self.__dataframe__[column])
        self.__dataframe__ = pd.concat([self.__dataframe__, one_hot_encoded], axis = 1)
        self.__dataframe__ = self.__dataframe__.drop(column, axis = 1)
        
        return self.__dataframe__


    def isolate(self, n_estimators: int = 500, return_scores = True):

        iForest = IsolationForest(n_estimators = n_estimators, verbose=2)
        iForest.fit(self.__dataframe__)

        if not return_scores:
            
            return iForest.predict(self.__dataframe__) 
        else:

            return -1 *iForest.score_samples(self.__dataframe__)
    
    def robust_scale(self, columns = [], train = True, scaler: RobustScaler = RobustScaler()) -> pd.DataFrame:

        subset_data = self.__dataframe__[columns]

        if train:
            
            scaler = RobustScaler()
            scaler = scaler.fit(subset_data)
            scaled_subset_data = scaler.transform(subset_data)
            
            # Replace the original subset of features with the scaled values
            self.__dataframe__[columns] = scaled_subset_data

            return self.__dataframe__, scaler
    
        else:

            scaled_subset_data = scaler.transform(subset_data)
            # Replace the original subset of features with the scaled values
            self.__dataframe__[columns] = scaled_subset_data

            return self.__dataframe__

        

    def apply_log(self, columns = []) -> pd.DataFrame:

        subset_data = self.__dataframe__[columns]

        # Apply logarithm transformation to the subset of features
        log_transformed_data = np.log(subset_data)

        # Replace the original subset of features with the transformed values
        self.__dataframe__[columns] = log_transformed_data


    def apply_log1p(self, columns = []) -> pd.DataFrame:

        subset_data = self.__dataframe__[columns]

        # Apply logarithm transformation to the subset of features
        log_transformed_data = np.log1p(subset_data)

        # Replace the original subset of features with the transformed values
        self.__dataframe__[columns] = log_transformed_data

    def filter(self, column, value, keep_smaller = True) -> pd.DataFrame:

        if keep_smaller:
            
            self.__dataframe__ = self.__dataframe__[self.__dataframe__[column] <= value]
            return self.__dataframe__
        else:
            self.__dataframe__ = self.__dataframe__[self.__dataframe__[column] >= value]
            return self.__dataframe__