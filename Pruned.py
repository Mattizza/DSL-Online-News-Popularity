import numpy as np
import importlib
from sklearn import metrics
import copy
import pandas as pd

from sklearn.model_selection import KFold, StratifiedKFold, ParameterGrid


class PrunedCV:

    def __init__(self, X_train: np.ndarray, y_train: np.ndarray, folds: KFold | StratifiedKFold):
        '''
        Initialize a new instance of the class. It creates a PrunedCV object that can be used to perform either model selection
        and model validation.

        Parameters
        ---
        X_train : np.array
            Data to cross-validate. It can be either the whole dataset (training/test) or just the training set
            (training/validation/set).

        y_train : np.array
            Labels to cross-validate, same distinction as `X_train`.
        
        folds : KFold | StratifiedKFold
            Already built cross-validator object.
        '''

        self.__folds__   = folds
        self.__X_train__ = X_train
        self.__y_train__ = y_train

    def set_params(self, param_grid: dict, scores: list) -> None:
        '''
        Pass the hyperparameters to tune and the scores used to choose the best ones. Notice that also a `sklearn` model is
        needed. All the information must be stored as nested dictionaries. Examples in the following.

        Parameters
        ---
        param_grid : dict
            Dictionary containing the model(s) and the hyperparameters.

        scores : list
            List containing all the scores we want to use to evalute the models.
        
        Examples
        ---
        >>> param_grid = { 
        >>> 'sklearn.linear_model.LogisticRegression': {
        >>>        'penalty': [None, 'l2'],
        >>>        'fit_intercept': [True, False],
        >>>        'C' : [0.001, 0.01, 0.1, 1, 10, 100],
        >>>        'solver': ['lbfgs', 'liblinear', 'saga']           
        >>>       },
        >>> 'sklearn.svm.SVC' : {
        >>>        'C' : [0.001, 0.01, 0.1, 1, 10, 100],
        >>>        'kernel' : ['linear', 'poly', 'rbf'],
        >>>       }                                     
        >>>    }
        '''

        self.__param_grid__ = param_grid
        self.__scores__     = scores

    def set_evaluation(self, score: metrics = metrics.accuracy_score, 
                       thresh_skip: int = 0, thresh_percentage: float = 0.0) -> None:
        '''
        Set the evaluation scores used during the procedure. It is also possible to set thresholds regarding the pruning steps.
        If no thresholds are passed, the pruning step will not be implemented.

        Parameters
        ---
        score : list, default = [accuracy_score]
            List of scores used to evaluate the goodness of a model. They have to be `sklearn.metrcis` methods.
         
        thresh_skip : int, default = 0
            How many times the actual model should be below the percentage of the maximal value in order to early stop the
            cross-validation and go to the next configuration.
        
        thresh_percentage : float, default = 0.0
            The percentage of the maximal value which is needed to be reached to continue cross-validating with that specific
            model. It can greatly lower the computational cost, but if too small it can also skip potentially good models.            
        '''

        self.__thresh_skip__       = thresh_skip
        self.__thresh_percentage__ = thresh_percentage
        self.__score__             = score.__name__


    def preprocess(X, y, train = True, means: dict = {}):
    
        if train:
            X['shares'] = y
            X = copy.deepcopy(X[(X['shares'] > 300) & (X['shares'] < 22000)])
            y = copy.deepcopy(y[(y > 300) & (y < 22000)])
            X = X.drop('shares', axis = 1)

        conditions = [
                (np.log1p(X['kw_avg_max']) < 2),
                ((np.log1p(X['kw_avg_max']) < 11) & (np.log1p(X['kw_avg_max']) > 0)),
                (np.log1p(X['kw_avg_max']) > 11)
                ]

        labels = ['kw_avg_max_none', 'kw_avg_max_medium', 'kw_avg_max_high']

        columns_to_fill = ['num_imgs', 'num_videos', 'num_keywords']
        dict_means = {}

        if train:
            X[columns_to_fill] = X[columns_to_fill].fillna(X[columns_to_fill].mean())
            dict_means = {'num_imgs'     : X['num_imgs'].mean(),
                        'num_videos'   : X['num_videos'].mean(),
                        'num_keywords' : X['num_keywords'].mean()}
        
        else:
            X = X.fillna(means)

        X['kw_avg_max'] = np.where(conditions[0], labels[0],
                                np.where(conditions[1], labels[1],
                                            np.where(conditions[2], labels[2], None)))
        
        X['weekday'] = np.where(X['weekday'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']), 'Not Weekend', 'Weekend')

        if train:
            y = y[X['n_tokens_content'] != 0]
            X = X[X['n_tokens_content'] != 0]

            y = y[np.log1p(X['num_hrefs']) < 4]
            y = y[np.log1p(X['num_self_hrefs']) < 3]
            y = y[X['self_reference_avg_sharess'] < 50000]
            
            X = X[np.log1p(X['num_hrefs']) < 4]
            X = X[np.log1p(X['num_self_hrefs']) < 3]
            X = X[X['self_reference_avg_sharess'] < 50000]
            
        X['title_sentiment_polarity'] = pd.cut(X['title_sentiment_polarity'],
                                            bins=[-1.00001, -0.5, -0.000000001, +0.000000001, 0.5, 1],
                                            labels=['high_negative_polarity', 'low_negative_polarity', 'neutral_polarity', 'low_positive_polarity', 'high_positive_polarity'],
                                            right = True)
        
        X['title_subjectivity'] = pd.cut(X['title_subjectivity'],
                                        bins = [-0.000001, 0.000001, 0.334, 0.667, 1],
                                        labels = ['no_subjectivity', 'low_subjectivity', 'medium_subjectvity', 'high_subjectivity'],
                                        right = True)

        X_processed = X.drop(['kw_max_min', 'kw_max_avg', 'kw_min_min', 'url', 'timedelta', 'n_non_stop_words',
                            'n_tokens_content', 'n_non_stop_unique_tokens', 'self_reference_max_shares',
                            'self_reference_min_shares', 'rate_positive_words', 'rate_negative_words',
                            'max_positive_polarity', 'min_positive_polarity', 'min_negative_polarity',
                            'max_negative_polarity', 'abs_title_subjectivity', 'abs_title_sentiment_polarity',
                            'kw_min_max', 'kw_max_max', 'kw_min_avg'], axis = 1)
        
        X_processed['num_hrefs'] = np.log1p(X_processed['num_hrefs'])
        X_processed['num_self_hrefs'] = np.log1p(X_processed['num_self_hrefs'])
        X_processed['num_imgs'] = np.log1p(X_processed['num_imgs'])
        X_processed['num_videos'] = np.log1p(X_processed['num_videos'])
        X_processed['kw_avg_min'] = np.log1p(X_processed['kw_avg_min'] + 1)
        X_processed['kw_avg_avg'] = np.log1p(X_processed['kw_avg_avg'])
        X_processed['self_reference_avg_sharess'] = np.log1p(X_processed['self_reference_avg_sharess'])
        
        y_processed = np.log(y)

        one_hot_encoded = pd.get_dummies(X_processed['data_channel'])

        # Concatenate the one-hot encoded columns with the original DataFrame
        X_processed = pd.concat([X_processed, one_hot_encoded], axis = 1)
        X_processed  = X_processed.drop('data_channel', axis = 1)

        one_hot_encoded = pd.get_dummies(X_processed['weekday'])

        # Concatenate the one-hot encoded columns with the original DataFrame
        X_processed = pd.concat([X_processed, one_hot_encoded], axis = 1)
        X_processed  = X_processed.drop('weekday', axis = 1)

        one_hot_encoded = pd.get_dummies(X_processed['kw_avg_max'])

        # Concatenate the one-hot encoded columns with the original DataFrame
        X_processed = pd.concat([X_processed, one_hot_encoded], axis = 1)
        X_processed  = X_processed.drop('kw_avg_max', axis = 1)

        one_hot_encoded = pd.get_dummies(X_processed['title_subjectivity'])

        # Concatenate the one-hot encoded columns with the original DataFrame
        X_processed = pd.concat([X_processed, one_hot_encoded], axis = 1)
        X_processed  = X_processed.drop('title_subjectivity', axis = 1)

        one_hot_encoded = pd.get_dummies(X_processed['title_sentiment_polarity'])

        # Concatenate the one-hot encoded columns with the original DataFrame
        X_processed = pd.concat([X_processed, one_hot_encoded], axis = 1)
        X_processed  = X_processed.drop('title_sentiment_polarity', axis = 1)

        return X_processed, y_processed, dict_means



    def __evaluate_model__(self, X_train: np.ndarray, y_train: np.ndarray, X_test: np.ndarray, y_test: np.ndarray,
                           model: type, scores: list) -> dict:
        

        model.fit(X_train, y_train)                  
        y_hat = model.predict(X_test)    

        # Exploit list comprehension to compute each score and store the results into a dictionary where each key is a score.
        results     = [np.sqrt(score(np.exp(y_test), np.exp(y_hat))) for score in scores]
        dic_results =  {k: v for k, v in zip([score.__name__ for score in scores], results)}
        
        return dic_results

    def do_cross_validation(self, verbose: int = 0) -> dict:
        '''
        This method just starts the cross-validation procedure.

        Parameters
        ---
        verbose : int, default = 2
            Specifies how much to print. The higher, the more details.
            * 0 = nothing will be printed;
            * 1 = information about the current model;
            * 2 = new print at every new configuration;
            * 3 = print the configuration;
            * 4 = print any detail about the folds.
        '''

        # Initialize best score and models performances.
        best               = 0
        models_performance = {}

        # Iterate over all models of interest.
        for model_str in self.__param_grid__.keys():                      

            count_config = 0                                   
            model_name   = model_str.split('.')[-1] 

            # Used to retrieve the method and not just the string.
            module = importlib.import_module(".".join(model_str.split('.')[:-1]))     
            model  = getattr(module, model_name)                                     
                                                                                    
            models_performance[model_name] = {}
            print(f"Model: {model_str}\n") if verbose >= 1 else None
            
            # For each model, iterate over all possible configuration.
            for config in ParameterGrid(self.__param_grid__[model_str]):                                                                         
                                                        
                print("\n\tNEW CONFIGURATION")        if verbose >= 2 else None
                print(f"\nConfiguration: {config}\n") if verbose >= 3 else None

                model_config_name = model_name + f"_{count_config}"

                # Initialize the scores and the weight for each fold. Each list will be updated later.
                models_performance[model_name][model_config_name] = {score: [] \
                                                                     for score in [score.__name__ for score in self.__scores__]}
                models_performance[model_name][model_config_name]['weight'] = []                                         
                                
                skipped    = False
                count_skip = 0                                                                                     
                count_fold = 1

                # Start the evaluation of the model using the folds.
                for train_indices, valid_indices in self.__folds__.split(self.__X_train__, self.__y_train__):
                    
                    # If the model has already reached bad performances #thresh_skip times, early terminate the process.
                    if count_skip == self.__thresh_skip__ and self.__thresh_skip__ != 0.0:

                        skipped = True
                        break
                                                                    
                    X_train_fold = self.__X_train__[train_indices]      
                    y_train_fold = self.__y_train__[train_indices]      
                    X_valid_fold = self.__X_train__[valid_indices]      
                    y_valid_fold = self.__y_train__[valid_indices]

                    X_train_fold, y_train_fold, means = self.preprocess(X_train_fold, y_train_fold, train = True)
                    X_valid_fold, y_valid_fold, _     = self.preprocess(X_train_fold, y_train_fold, train = False, means = means)
                    

                    # Train the classifier and evaluate it.                                                      
                    clf = model(**config)                                           
                    results = self.__evaluate_model__(X_train_fold, y_train_fold, X_valid_fold, y_valid_fold,
                                                      clf, self.__scores__)
                    
                    # Append the scores to the dictionary.
                    for score, result in results.items():
                        
                        models_performance[model_name][model_config_name][score].append(result)

                    # Store the weight of the score, since different amount of samples per fold may occur.
                    models_performance[model_name][model_config_name]['weight'].append(len(train_indices))
                    
                    # Every new fold, compute the average of the scores.
                    actual_scores          = models_performance[model_name][model_config_name][self.__score__]
                    actual_weights         = models_performance[model_name][model_config_name]['weight']
                    actual_avg_performance = np.average(actual_scores, weights = actual_weights)

                    # If the model has a bad performance, increase the count by 1.
                    count_skip += 1 if actual_avg_performance < self.__thresh_percentage__ * best else 0
                                        
                    if verbose >= 4:
                        
                        print(f"Fold {count_fold} / {self.__folds__.get_n_splits()} - Skip: {count_skip} / {self.__thresh_skip__}")
                        print(f"Results: {results}")
                        print(f"Highest average {self.__score__}: {np.round(best, 4)}")

                    count_fold += 1
                
                total_scores          = models_performance[model_name][model_config_name][self.__score__]
                total_weights         = models_performance[model_name][model_config_name]['weight']
                total_avg_performance = np.average(total_scores, weights = total_weights)
                

                # If the model has really good performances, it becomes the new best.
                best = total_avg_performance if total_avg_performance >= best and self.__thresh_percentage__ != 0.0 else best

                # Store the parameters in order to be able later to retrieve the best configuration.
                models_performance[model_name][model_config_name]['parameters'] = config
                models_performance[model_name][model_config_name]['skipped'] = skipped
                count_config += 1

            print('\n')

        self.models_perfomance = models_performance
    
    def get_performance(self) -> dict:
        '''
        Just returns the performances.
        '''
        return self.models_perfomance