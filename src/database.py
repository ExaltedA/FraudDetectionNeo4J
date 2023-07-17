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
        self.logger.addHandler(logging.FileHandler(f"{self.dir_output}/log.txt"))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def load_customer(self, path):
        with self.driver.session() as session:
            query =  (
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

    def load_transaction(self, path):
        with self.driver.session() as session:
            query = (
                "LOAD CSV WITH HEADERS FROM $path AS row "
                "CALL { "
                "    WITH row "
                "    WITH toInteger(row.TRANSACTION_ID) AS TRANSACTION_ID, "
                "         datetime(replace(row.TX_DATETIME,' ','T')) AS TX_DATETIME, "
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

            self.logger.info(f"Load transaction csv from {path}")
            result = session.run(query, path=path)
            summary = result.consume()
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            self.logger.info(f"Time: {total_time} ms")

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
                "WITH c, t, datetime.truncate('week', t.TX_DATETIME) AS week, "
                "     CASE WHEN datetime().month < 7 "
                "     THEN 1 ELSE 7 END AS semesterStart "
                "WITH c, t, week, semesterStart, (semesterStart+5) AS semesterEnd "
                "WHERE t.TX_DATETIME >= datetime({ year:datetime().year-1, month:semesterStart, day:1 }) "
                "      AND t.TX_DATETIME <= datetime({ year:datetime().year-1, month:semesterEnd, day:1 }) "
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
                "CASE WHEN trans.TX_DATETIME.month<7 "
                "     THEN 'first' "
                "     ELSE 'second' END AS semester "
                "WITH term.TERMINAL_ID AS terminal, year, semester, AVG(trans.TX_AMOUNT) AS avg_amount "
                "MATCH (t:Terminal { TERMINAL_ID: terminal })-[EXECUTE]->(tr:Transaction "
                "WHERE "
                "     CASE WHEN tr.TX_DATETIME.month<7 "
                "          THEN year-1 = tr.TX_DATETIME.year "
                "          ELSE year = tr.TX_DATETIME.year END "
                "     AND "
                "     CASE WHEN tr.TX_DATETIME.month<7 "
                "          THEN semester = 'second' "
                "          ELSE semester = 'first' END "
                "     AND (tr.TX_AMOUNT > 1.1*avg_amount OR tr.TX_AMOUNT < 0.9*avg_amount)) "
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
                "MATCH (terminal: Terminal)-[execute:EXECUTE]->(transaction: Transaction)<-[make:MAKE]-(c: Customer) "
                "CALL { "
                "    WITH c, terminal "
                "    MERGE (c)-[r:USE]->(terminal) "
                "} IN TRANSACTIONS;"
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
                "MATCH (user1:Customer)-[:USE*4]-(user2:Customer) "
                "WHERE id(user1) < id(user2) "
                "RETURN DISTINCT user1.CUSTOMER_ID, user2.CUSTOMER_ID;"
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
                "CALL { "
                "    WITH t "
                "    WITH "
                "    CASE "
                "         WHEN t.transactionDate.hour >= 0 AND t.transactionDate.hour < 6 THEN 'night' "
                "         WHEN t.transactionDate.hour >= 6 AND t.transactionDate.hour < 12 THEN 'morning' "
                "         WHEN t.transactionDate.hour >= 12 AND t.transactionDate.hour < 18 THEN 'afternoon' "
                "    ELSE 'evening' "
                "    END AS period, t "
                "    SET t.period = period"
                "} IN TRANSACTIONS;"
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
                "CALL { "
                "    WITH t "
                "    WITH apoc.text.random(1, '12345') AS productCode, t "
                "    WITH "
                "    CASE productCode "
                "    WHEN '1' THEN 'high-tech' "
                "    WHEN '2' THEN 'food' "
                "    WHEN '3' THEN 'clothing' "
                "    WHEN '4' THEN 'consumable' "
                "    ELSE 'other' "
                "    END AS product, t "
                "    SET t.product = product "
                "} IN TRANSACTIONS;"
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
                "MATCH (user1:Customer)-[:BUYING_FRIEND*4]-(user2:Customer) "
                "WHERE id(user1) < id(user2) "
                "RETURN DISTINCT user1.CUSTOMER_ID, user2.CUSTOMER_ID;"
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
