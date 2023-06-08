import pandas as pd
import numpy as np
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder

class Preprocessing:
    
    def __init__(self):
        self.mapping = None
        self.drop_features = ['kw_max_min', 'kw_max_avg', 'kw_min_min', 'url', 'timedelta', 'n_non_stop_words',
                              'n_tokens_content', 'n_non_stop_unique_tokens', 'self_reference_max_shares',
                              'self_reference_min_shares', 'rate_positive_words', 'rate_negative_words',
                              'max_positive_polarity', 'min_positive_polarity', 'min_negative_polarity',
                              'max_negative_polarity', 'abs_title_subjectivity', 'abs_title_sentiment_polarity',
                              'kw_min_max', 'kw_max_max', 'kw_min_avg']
        self.log1p_features = ['num_hrefs', 'num_self_hrefs', 'num_imgs', 'num_videos', 'kw_avg_min', 'kw_avg_avg',
                               'self_reference_avg_sharess']
        self.log_feature = 'shares'
        self.subjectivity_bins = [-0.000001, 0.000001, 0.334, 0.667, 1]
        self.polarity_bins = [-1.00001, -0.5, -0.000000001, +0.000000001, 0.5, 1]
        self.subjectivity_bin_values = {
            1: 'no_subjectivity',
            2: 'low_subjectivity',
            3: 'medium_subjectvity',
            4: 'high_subjectivity'
        }
        self.polarity_bin_values = {
            1: 'high_negative_polarity',
            2: 'low_negative_polarity',
            3: 'neutral_polarity',
            4: 'low_positive_polarity',
            5: 'high_positive_polarity'
        }

    def _drop_features(self, X):
        return X.drop(columns=self.drop_features)

    def _apply_log1p(self, X):
        return np.log1p(X)

    def _apply_log(self, X):
        return np.log(X)

    def _discretize_title_subjectivity(self, X):
        bin_indices = np.digitize(X[:, 0], self.subjectivity_bins, right=True)
        bin_labels = np.array([self.subjectivity_bin_values[i] for i in bin_indices]).reshape(-1, 1)
        return bin_labels

    def _discretize_title_sentiment_polarity(self, X):
        bin_indices = np.digitize(X[:, 0], self.polarity_bins, right=True)
        bin_labels = np.array([self.polarity_bin_values[i] for i in bin_indices]).reshape(-1, 1)
        return bin_labels

    def preprocess(self, df):
        self.mapping = {value: index for index, value in enumerate(df.columns)}

        # DROP FEATURES
        df['kw_avg_min'] = df['kw_avg_min'] + 1
        df = self._drop_features(df)

        # APPLY LOGARITHMS
        df[self.log1p_features] = self._apply_log1p(df[self.log1p_features])
        df[self.log_feature] = self._apply_log(df[self.log_feature])

        # DISCRETIZE KW_AVG_MAX
        conditions = [
            (np.log(df['kw_avg_max'] + 1) < 2),
            ((np.log(df['kw_avg_max'] + 1) < 11) & (np.log(df['kw_avg_max'] + 1) > 0)),
            (np.log(df['kw_avg_max'] + 1) > 11)
        ]
        labels = ['kw_avg_max_none', 'kw_avg_max_medium', 'kw_avg_max_high']
        df['kw_avg_max'] = np.select(conditions, labels, None)

        # DISCRETIZE TITLE_SUBJECTIVITY AND TITLE_SENTIMENT_POLARITY
        df['title_subjectivity'] = pd.cut(df['title_subjectivity'],
                                          bins=[-1.00001, -0.5, -0.000000001, +0.000000001, 0.5, 1],
                                          labels=['high_negative_polarity', 'low_negative_polarity',
                                                  'neutral_polarity', 'low_positive_polarity',
                                                  'high_positive_polarity'],
                                          right=True)
        df['title_sentiment_polarity'] = pd.cut(df['title_sentiment_polarity'],
                                                bins=[-1.00001, -0.5, -0.000000001, +0.000000001, 0.5, 1],
                                                labels=['high_negative_polarity', 'low_negative_polarity',
                                                        'neutral_polarity', 'low_positive_polarity',
                                                        'high_positive_polarity'],
                                                right=True)

        # DISCRETIZE WEEKDAY
        df['weekday'] = np.where(df['weekday'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']),
                                 'Not Weekend', 'Weekend')

        # ONE-HOT ENCODING
        categorical_features = ['weekday', 'title_subjectivity', 'title_sentiment_polarity', 'kw_avg_max']
        encoder = OneHotEncoder(categories='auto', sparse=False, handle_unknown='ignore')
        encoded_features = encoder.fit_transform(df[categorical_features].values)

        # DROP ORIGINAL CATEGORICAL COLUMNS
        df = df.drop(columns=categorical_features)

        # CONCATENATE ENCODED FEATURES WITH ORIGINAL DATASET
        df_encoded = pd.DataFrame(encoded_features,
                                  columns=encoder.get_feature_names_out(categorical_features))
        df = pd.concat([df, df_encoded], axis=1)

        return df