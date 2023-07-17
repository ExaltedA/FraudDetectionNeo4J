import numpy as np
import pandas as pd
import time
import random
import logging
import os

logger = logging.getLogger("generator")

# Creates directory if there is no
def dir_error_handler(path):
    if not os.path.exists(path):
        os.makedirs(path)


# Converts datafrane to csv
def convert_df_to_csv(data, path, name):
    data.to_csv(f"{path}{name}.csv", index=False)


def generate_customer_profiles_table(n_customers, random_state=0):

    np.random.seed(random_state)

    customer_id_properties = []

    # Generate customer properties from random distributions
    for customer_id in range(n_customers):

        x_customer_id = np.random.uniform(0, 100)
        y_customer_id = np.random.uniform(0, 100)

        # Arbitrary (but sensible) value
        mean_amount = np.random.uniform(5, 100)
        std_amount = mean_amount/2  # Arbitrary (but sensible) value

        mean_nb_tx_per_day = np.random.uniform(
            0, 4)  # Arbitrary (but sensible) value

        customer_id_properties.append([customer_id,
                                      x_customer_id, y_customer_id,
                                      mean_amount, std_amount,
                                      mean_nb_tx_per_day])

    customer_profiles_table = pd.DataFrame(customer_id_properties, columns=['CUSTOMER_ID',
                                                                            'x_customer_id', 'y_customer_id',
                                                                            'mean_amount', 'std_amount',
                                                                            'mean_nb_tx_per_day'])

    return customer_profiles_table


def generate_terminal_profiles_table(n_terminals, random_state=0):

    np.random.seed(random_state)

    terminal_id_properties = []

    # Generate terminal properties from random distributions
    for terminal_id in range(n_terminals):

        x_terminal_id = np.random.uniform(0, 100)
        y_terminal_id = np.random.uniform(0, 100)

        terminal_id_properties.append([terminal_id,
                                      x_terminal_id, y_terminal_id])

    terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                            'x_terminal_id', 'y_terminal_id'])

    return terminal_profiles_table


def get_list_terminals_within_radius(customer_profile, x_y_terminals, radius):

    # Use numpy arrays in the following to speed up computations

    # Location (x,y) of customer as numpy array
    x_y_customer = customer_profile[[
        'x_customer_id', 'y_customer_id']].values.astype(float)

    # Squared difference in coordinates between customer and terminal locations
    squared_diff_x_y = np.square(x_y_customer - x_y_terminals)

    # Sum along rows and compute suared root to get distance
    dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))

    # Get the indices of terminals which are at a distance less than r
    available_terminals = list(np.where(dist_x_y < radius)[0])

    # Return the list of terminal IDs
    return available_terminals


def generate_transactions_table(customer_profile, start_date, number_of_days):

    customer_transactions = []

    random.seed(int(customer_profile.CUSTOMER_ID))
    np.random.seed(int(customer_profile.CUSTOMER_ID))

    # For all days
    for day in range(number_of_days):

        # Random number of transactions for that day
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)

        # If nb_tx positive, let us generate transactions
        if nb_tx > 0:

            for tx in range(nb_tx):

                # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that
                # most transactions occur during the day.
                time_tx = int(np.random.normal(86400/2, 20000))

                # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
                if (time_tx > 0) and (time_tx < 86400):

                    # Amount is drawn from a normal distribution
                    amount = np.random.normal(
                        customer_profile.mean_amount, customer_profile.std_amount)

                    # If amount negative, draw from a uniform distribution
                    if amount < 0:
                        amount = np.random.uniform(
                            0, customer_profile.mean_amount*2)

                    amount = np.round(amount, decimals=2)

                    if len(customer_profile.available_terminals) > 0:

                        terminal_id = random.choice(
                            customer_profile.available_terminals)

                        customer_transactions.append([time_tx+day*86400, day,
                                                      customer_profile.CUSTOMER_ID,
                                                      terminal_id, amount])

    customer_transactions = pd.DataFrame(customer_transactions, columns=[
                                         'TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT'])

    if len(customer_transactions) > 0:
        customer_transactions['TX_DATETIME'] = pd.to_datetime(
            customer_transactions["TX_TIME_SECONDS"], unit='s', origin=start_date)
        customer_transactions = customer_transactions[[
            'TX_DATETIME', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS']]

    return customer_transactions


def add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df):

    # By default, all transactions are genuine
    transactions_df['TX_FRAUD'] = 0
    transactions_df['TX_FRAUD_SCENARIO'] = 0

    # Scenario 1
    start_time = time.time()
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD'] = 1
    transactions_df.loc[transactions_df.TX_AMOUNT >
                        220, 'TX_FRAUD_SCENARIO'] = 1
    nb_frauds_scenario_1 = transactions_df.TX_FRAUD.sum()
    logger.info("Time to generate frauds from scenario 1:  {0:>8.2f}s".format(
        time.time()-start_time))
    logger.info("Number of frauds from scenario 1: "+str(nb_frauds_scenario_1))

    # Scenario 2
    start_time = time.time()
    for day in range(transactions_df.TX_TIME_DAYS.max()):

        compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(
            n=2, random_state=day)

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day+28) &
                                                   (transactions_df.TERMINAL_ID.isin(compromised_terminals))]

        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD'] = 1
        transactions_df.loc[compromised_transactions.index,
                            'TX_FRAUD_SCENARIO'] = 2

    nb_frauds_scenario_2 = transactions_df.TX_FRAUD.sum()-nb_frauds_scenario_1
    logger.info("Time to generate frauds from scenario 2:  {0:>8.2f}s".format(
        time.time()-start_time))
    logger.info("Number of frauds from scenario 2: "+str(nb_frauds_scenario_2))

    # Scenario 3
    start_time = time.time()
    for day in range(transactions_df.TX_TIME_DAYS.max()):

        compromised_customers = customer_profiles_table.CUSTOMER_ID.sample(
            n=3, random_state=day).values

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day+14) &
                                                   (transactions_df.CUSTOMER_ID.isin(compromised_customers))]

        nb_compromised_transactions = len(compromised_transactions)

        random.seed(day)
        index_fauds = random.sample(list(compromised_transactions.index.values), k=int(
            nb_compromised_transactions/3))

        transactions_df.loc[index_fauds,
                            'TX_AMOUNT'] = transactions_df.loc[index_fauds, 'TX_AMOUNT']*5
        transactions_df.loc[index_fauds, 'TX_FRAUD'] = 1
        transactions_df.loc[index_fauds, 'TX_FRAUD_SCENARIO'] = 3

    nb_frauds_scenario_3 = transactions_df.TX_FRAUD.sum()-nb_frauds_scenario_2 - \
        nb_frauds_scenario_1
    logger.info("Time to generate frauds from scenario 3:  {0:>8.2f}s".format(
        time.time()-start_time))
    logger.info("Number of frauds from scenario 3: "+str(nb_frauds_scenario_3))

    return transactions_df


def generate_dataset(n_customers, n_terminals, number_of_days, start_date, radius):

    start_time = time.time()
    customer_profiles_table = generate_customer_profiles_table(
        n_customers, random_state=0)
    logger.info("Time to generate customer profiles table: {0:>8.2f}s".format(
        time.time()-start_time))

    start_time = time.time()
    terminal_profiles_table = generate_terminal_profiles_table(
        n_terminals, random_state=1)
    logger.info("Time to generate terminal profiles table: {0:>8.2f}s".format(
        time.time()-start_time))

    start_time = time.time()
    x_y_terminals = terminal_profiles_table[[
        'x_terminal_id', 'y_terminal_id']].values.astype(float)
    customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
        lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, radius=radius), axis=1)
    # With Pandarallel
    # customer_profiles_table['available_terminals'] = customer_profiles_table.parallel_apply(lambda x : get_list_closest_terminals(x, x_y_terminals=x_y_terminals, r=r), axis=1)
    customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(
        len)
    logger.info("Time to associate terminals to customers: {0:>8.2f}s".format(
        time.time()-start_time))

    start_time = time.time()
    transactions_df = customer_profiles_table.groupby('CUSTOMER_ID').apply(lambda x: generate_transactions_table(
        x.iloc[0], start_date=start_date, number_of_days=number_of_days)).reset_index(drop=True)
    # With Pandarallel
    # transactions_df=customer_profiles_table.groupby('CUSTOMER_ID').parallel_apply(lambda x : generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)
    logger.info("Time to generate transactions:            {0:>8.2f}s".format(
        time.time()-start_time))

    # Sort transactions chronologically
    transactions_df = transactions_df.sort_values('TX_DATETIME')
    # Reset indices, starting from 0
    transactions_df.reset_index(inplace=True, drop=True)
    transactions_df.reset_index(inplace=True)
    # TRANSACTION_ID are the dataframe indices, starting from 0
    transactions_df.rename(columns={'index': 'TRANSACTION_ID'}, inplace=True)

    transactions_df = add_frauds(
        customer_profiles_table, terminal_profiles_table, transactions_df)

    return (customer_profiles_table, terminal_profiles_table, transactions_df)


def generate_all_datasets(template, dir_data, start_date, radius, force=False):
    dataset_template = template
    file_names = ("customer", "terminal", "transaction")

    for k, v in dataset_template.items():
        path = f"{dir_data}/{k}/"
        if force or not any([os.path.isfile(f"{path}{name}.csv") for name in file_names]):
            logger.info(f"Generate {k}Mbyte dataset")

            customers, terminals, number_of_days = v[0], v[1], v[2]
            datasets = dict(zip(file_names,
                                generate_dataset(n_customers=customers,
                                                 n_terminals=terminals,
                                                 number_of_days=number_of_days,
                                                 start_date=start_date,
                                                 radius=radius)))

            dir_error_handler(path)

            for name, data in datasets.items():
                convert_df_to_csv(data, path, name)
