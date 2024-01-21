import os
from neo4j import GraphDatabase
import logging


class Database:

    def __init__(self, uri, user, password, dir_output):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.dir_output = dir_output

        if not os.path.exists(self.dir_output):
            os.makedirs(self.dir_output)

        self.logger = logging.getLogger(uri)
        open(f"{self.dir_output}/log.txt", "w")
        self.logger.addHandler(logging.FileHandler(
            f"{self.dir_output}/log.txt"))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def load_customer(self, path):
        with self.driver.session() as session:
            query = (
                "LOAD CSV WITH HEADERS FROM $path AS row "
                "WITH toInteger(row.CUSTOMER_ID) AS CUSTOMER_ID, "
                "     toFloat(row.x_customer_id) AS x_customer_id, "
                "     toFloat(row.y_customer_id) AS y_customer_id, "
                "     toFloat(row.mean_amount) AS mean_amount, "
                "     toFloat(row.std_amount) AS std_amount, "
                "     toFloat(row.mean_nb_tx_per_day) AS mean_nb_tx_per_day "
                "WHERE CUSTOMER_ID IS NOT NULL "
                "MERGE (c:Customer { CUSTOMER_ID : CUSTOMER_ID, "
                "                    x_customer_id : x_customer_id, "
                "                    y_customer_id : y_customer_id, "
                "                    mean_amount : mean_amount, "
                "                    std_amount : std_amount, "
                "                    mean_nb_tx_per_day : mean_nb_tx_per_day });"
            )

            self.logger.info(f"Load customer csv from {path}")
            result = session.run(query, path=path)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def load_terminal(self, path):
        with self.driver.session() as session:
            query = (
                "LOAD CSV WITH HEADERS FROM $path AS row "
                "WITH toInteger(row.TERMINAL_ID) AS TERMINAL_ID, "
                "     toFloat(row.x_terminal_id) AS x_terminal_id, "
                "     toFloat(row.y_terminal_id) AS y_terminal_id "
                "WHERE TERMINAL_ID IS NOT NULL "
                "MERGE (t:Terminal { TERMINAL_ID : TERMINAL_ID, "
                "                    x_terminal_id : x_terminal_id, "
                "                    y_terminal_id : y_terminal_id });"
            )

            self.logger.info(f"Load terminal csv from {path}")
            result = session.run(query, path=path)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def parse_csv_line(self, line):
        # Parse the line and create a dictionary
        # You'll need to customize this function based on your CSV structure
        # Example:
        columns = line.strip().split(',')
        data = {
            "TRANSACTION_ID": columns[0],
            "TX_DATETIME": columns[1],
            "TX_AMOUNT": columns[2],
            "TX_FRAUD": columns[3],
            "CUSTOMER_ID": columns[4],
            "TERMINAL_ID": columns[5]
        }
        return data

    def process_chunk_transaction(self, lines):
        query = (
            "UNWIND $batch AS row "
            "CALL { "
            "    WITH row "
            "    WITH toInteger(row.TRANSACTION_ID) AS TRANSACTION_ID, "
            "         datetime(replace(row.TX_DATETIME, ' ', 'T')) AS TX_DATETIME, "
            "         toFloat(row.TX_AMOUNT) AS TX_AMOUNT, "
            "         toInteger(row.TX_FRAUD) AS TX_FRAUD, "
            "         toInteger(row.CUSTOMER_ID) AS CUSTOMER_ID, "
            "         toInteger(row.TERMINAL_ID) AS TERMINAL_ID "
            "    WHERE TRANSACTION_ID IS NOT NULL "
            "    MATCH (terminal:Terminal { TERMINAL_ID: TERMINAL_ID }), "
            "          (customer:Customer { CUSTOMER_ID: CUSTOMER_ID }) "
            "    MERGE (terminal)-[execute:EXECUTE]-> "
            "          (t:Transaction { TRANSACTION_ID : TRANSACTION_ID, "
            "                           TX_DATETIME : TX_DATETIME, "
            "                           TX_AMOUNT : TX_AMOUNT, "
            "                           TX_FRAUD : TX_FRAUD }) "
            "          <-[make:MAKE]-(customer) "
            "} IN TRANSACTIONS;"
        )
        summary = 0
        with self.driver.session() as session:
            result = session.run(query, batch=lines)
            summary = result.consume()
        return summary

    def load_transaction(self, path):
        self.logger.info(f"Load transaction csv from {path}")
        chunk_size = 5000
        total_lines_processed = 0
        total_time_available = 0
        total_time_consumed = 0

        with open(path, 'r') as file:
            next(file)  # Skip header row
            lines = []
            for line in file:
                lines.append(self.parse_csv_line(line))
                if len(lines) == chunk_size:
                    summary = self.process_chunk_transaction(lines)
                    total_lines_processed += len(lines)
                    total_time_available += summary.result_available_after
                    total_time_consumed += summary.result_consumed_after
                    lines = []
                    self.logger.info(f"Processed {total_lines_processed} lines")

            # Process remaining lines
            if lines:
                summary = self.process_chunk_transaction(lines)
                total_lines_processed += len(lines)
                total_time_available += summary.result_available_after
                total_time_consumed += summary.result_consumed_after
                self.logger.info(f"Processed {total_lines_processed} lines")

        total_time = total_time_available + total_time_consumed
        self.logger.info(f"Total processed lines: {total_lines_processed}")
        self.logger.info(f"Total time: {total_time} ms")

    def index_customer(self):
        with self.driver.session() as session:
            query = (
                "CREATE INDEX customer_index IF NOT EXISTS "
                "FOR (c:Customer) ON (c.CUSTOMER_ID);"
            )

            self.logger.info(f"Create index on Customer.CUSTOMER_ID")
            result = session.run(query)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def index_terminal(self):
        with self.driver.session() as session:
            query = (
                "CREATE INDEX terminal_index IF NOT EXISTS "
                "FOR (t:Terminal) ON (t.TERMINAL_ID);"
            )

            self.logger.info(f"Create index on Terminal.TERMINAL_ID")
            result = session.run(query)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def index_transaction(self):
        with self.driver.session() as session:
            query = (
                "CREATE INDEX transaction_index IF NOT EXISTS "
                "FOR (tr:Transaction) ON (tr.TRANSACTION_ID);"
            )

            self.logger.info(f"Create index on Transaction.TRANSACTION_ID")
            result = session.run(query)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def query_1(self):
        with self.driver.session() as session:
            query = (
                "MATCH (c:Customer)-[:MAKE]->(t:Transaction) "
                "WHERE t.TX_DATETIME >= datetime({ year: datetime().year-1, month: CASE WHEN datetime().month < 7 THEN 1 ELSE 7 END, day: 1 }) "
                "   AND t.TX_DATETIME < datetime({ year: datetime().year, month: CASE WHEN datetime().month < 7 THEN 7 ELSE 1 END, day: 1 }) "
                "WITH c, t, datetime.truncate('week', t.TX_DATETIME) AS week "
                "RETURN c.CUSTOMER_ID AS customer, sum(t.TX_AMOUNT) AS amount, week "
                "ORDER BY customer, week;"
            )

            self.logger.info(f"Query 1")
            result = session.run(query)
            values = result.to_df()
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")
            self.logger.info(f"Results:\n{values}")

            values.to_csv(f"{self.dir_output}/Q1.csv", index=False)
            self.logger.info(f"Results saved in {self.dir_output}/Q1.csv")

    def query_2(self):
        with self.driver.session() as session:
            query = (
                "MATCH (term:Terminal)-[EXECUTE]->(trans:Transaction) "
                "WITH term, trans, trans.TX_DATETIME.year AS year, "
                "     CASE WHEN trans.TX_DATETIME.month < 7 THEN 'first' ELSE 'second' END AS semester "
                "WITH term.TERMINAL_ID AS terminal, year, semester, AVG(trans.TX_AMOUNT) AS avg_amount "
                "MATCH (t:Terminal { TERMINAL_ID: terminal })-[EXECUTE]->(tr:Transaction) "
                "WHERE (tr.TX_DATETIME.month < 7 AND year - 1 = tr.TX_DATETIME.year AND semester = 'second') OR "
                "      (tr.TX_DATETIME.month >= 7 AND year = tr.TX_DATETIME.year AND semester = 'first') "
                "      AND (tr.TX_AMOUNT > 1.1 * avg_amount OR tr.TX_AMOUNT < 0.9 * avg_amount) "
                "RETURN terminal, collect(tr.TRANSACTION_ID) AS transactions "
                "ORDER BY terminal;"
            )

            self.logger.info(f"Query 2")
            result = session.run(query)
            values = result.to_df()
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")
            self.logger.info(f"Results:\n{values}")

            values.to_csv(f"{self.dir_output}/Q2.csv", index=False)
            self.logger.info(f"Results saved in {self.dir_output}/Q2.csv")

    def query_3(self):
        with self.driver.session() as session:
            create_use = (
                "MATCH (terminal:Terminal)-[:EXECUTE]->(transaction:Transaction)<-[:MAKE]-(customer:Customer) "
                "MERGE (customer)-[:USE]->(terminal);"
            )

            self.logger.info(f"Create USE relationship")
            result = session.run(create_use)
            values = result.to_df()
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

            query = (
                "MATCH path = (u1:Customer)-[:USE*4]-(u2:Customer) "
                "WHERE id(u1) < id(u2) "
                "RETURN DISTINCT u1.CUSTOMER_ID AS Customer1, u2.CUSTOMER_ID AS Customer2;"
            )

            self.logger.info(f"Query 3")
            result = session.run(query)
            values = result.to_df()
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")
            self.logger.info(f"Results:\n{values}")

            values.to_csv(f"{self.dir_output}/Q3.csv", index=False)
            self.logger.info(f"Results saved in {self.dir_output}/Q3.csv")

    def query_4_1(self):
        with self.driver.session() as session:
            query = (
                "MATCH (t:Transaction) "
                "SET t.period = CASE "
                "   WHEN t.transactionDate.hour >= 0 AND t.transactionDate.hour < 6 THEN 'night' "
                "   WHEN t.transactionDate.hour >= 6 AND t.transactionDate.hour < 12 THEN 'morning' "
                "   WHEN t.transactionDate.hour >= 12 AND t.transactionDate.hour < 18 THEN 'afternoon' "
                "   ELSE 'evening' "
                "END;"
            )

            self.logger.info(f"Query 4.1")
            result = session.run(query)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def query_4_2(self):
        with self.driver.session() as session:
            query = (
                "MATCH (t:Transaction) "
                "SET t.product = CASE toInteger(rand() * 5) "
                "   WHEN 1 THEN 'high-tech' "
                "   WHEN 2 THEN 'food' "
                "   WHEN 3 THEN 'clothing' "
                "   WHEN 4 THEN 'consumable' "
                "   ELSE 'other' "
                "END;"
            )

            self.logger.info(f"Query 4.2")
            result = session.run(query)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def query_4_3(self):
        with self.driver.session() as session:
            query = (
                "MATCH(c:Customer)-[:MAKE]->(tr:Transaction)<-[:EXECUTE]-(t:Terminal) "
                "WITH c, tr, t "
                "WITH c AS customer, t.TERMINAL_ID as terminal, tr.product AS product, COUNT(tr) AS numb_tr "
                "WHERE numb_tr > 3 "
                "WITH terminal, product, collect(customer) AS customers "
                "WITH DISTINCT customers, terminal "
                "UNWIND apoc.coll.combinations(customers, 2) as pair "
                "WITH pair[0] as first, pair[1] as second "
                "MERGE (first)-[:BUYING_FRIEND]-(second);"
            )

            self.logger.info(f"Query 4.3")
            result = session.run(query)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

    def query_5(self):
        with self.driver.session() as session:
            query = (
                "MATCH path = (c:Customer)-[:BUYING_FRIEND*4]-(friend:Customer) "
                "WHERE c <> friend "
                "RETURN c.CUSTOMER_ID AS CustomerID, friend.CUSTOMER_ID AS FriendID, LENGTH(path) AS Degree "
                "ORDER BY CustomerID, FriendID;"
            )

            self.logger.info(f"Query 5")
            result = session.run(query)
            values = result.to_df()
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")
            self.logger.info(f"Results:\n{values}")

            values.to_csv(f"{self.dir_output}/Q5.csv", index=False)
            self.logger.info(f"Results saved in {self.dir_output}/Q5.csv")
