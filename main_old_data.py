import requests
import os
from datetime import date, timedelta
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as f
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, DateType, ArrayType

# vacancies_schema = StructType([
#     StructField('vacancy_id', IntegerType(), True),
#     StructField('city_id', IntegerType(), True),
#     StructField('profession_id', IntegerType(), True),
#     StructField('salary_currency', StringType(), True),
#     StructField('salary_from', DoubleType(), True),
#     StructField('salary_to', DoubleType(), True),
#     StructField('experience', StringType(), True),
#     StructField('shedule', StringType(), True),
#     StructField('skills', ArrayType(StringType()), True),
#     StructField('dt', DateType(), True)
# ])

def get_config(logger):
    config = None
    try:
        with open(".env", 'r', encoding='utf-8') as f:
            config =  {x.strip().split('=')[0]:x.strip().split('=')[1] for x in f.readlines()}
            logger.info("Read config")
            return config
    except:
        logger.error(".env file is missing")

def get_table(spark: SparkSession, name_table, config, logger):
    if config:
        try:
            logger.info(f"Get data from {name_table}")
            connection_properties = {
                        'user': config['DB_USER'],
                        'password': config['DB_PASSWORD'],
                        'driver': 'org.postgresql.Driver'}
            table = spark.read.jdbc(
                url = f"jdbc:postgresql://{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}",
                table = name_table,
                properties = connection_properties
            )
            return table
        except:
            logger.error("Error with Postgres")

def load_data(df: DataFrame, name_table, config, logger):
    if config:
        try:
            connection_properties = {
                'user': config['DB_USER'],
                'password': config['DB_PASSWORD'],
                'driver': 'org.postgresql.Driver'}

            df.write.mode("append").jdbc(
                url = f"jdbc:postgresql://{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}",
                table = name_table,
                properties = connection_properties)
            logger.info(f"Data load to {name_table} successfully")
        except:
            logger.error("Error load to databse")

def run_parse(profession_name, config, logger):
    try:
        logger.info("Start parse")
        url = f"http://{config['PARSE_HOST']}:{config['PARSE_PORT']}/parse"
        headers = {'Content-type': 'application/json',
                'Accept': 'text/plain',
                'Content-Encoding': 'utf-8'}
        data = {'profession':profession_name
            ,'city_id':'1'
            ,'date': str(date.today() - timedelta(days=1))}
        answer = requests.post(url, headers=headers, json=data)
        logger.info(f"answer {str(answer.status_code)}")
        answer.close()
        logger.info("End of parsing")
    except:
        logger.error("Error parsing")

def transform_to_metrics(vacancies, profession_name, logger):
    try:


        vacancies = (vacancies.withColumn("lower_name", 
                        f.replace(f.lower(f.col('profession')), 
                                f.lit('-'), 
                                f.lit(" ")))
            .filter(((f.contains(f.col("lower_name"), 
                                f.lit(profession_name[0]))) | 
                    (f.contains(f.col("lower_name"), 
                                f.lit(profession_name[1])))) &
                    (f.col("dt_t").isNotNull())
                                )
            .withColumn("profession_id", f.lit(profession_name[2]))
            .withColumn("salary", ((f.col("salary_from")+ f.col("salary_to")) / 2))
            .select(
                f.col("vacancy_id"),
                f.col("city_id"),
                f.col("profession_id"),
                f.col("experience"),
                f.col("schedule"),
                f.col("skills"),
                f.col("salary"),
                f.col("dt_t").alias("dt")))
        logger.info("Transform data vacancies")
        

        experience = (vacancies.groupBy(f.col("city_id"),
                                        f.col("profession_id"),
                                        f.col("dt")
                                        )
                                .pivot("experience",["noExperience", "between1And3", "between3And6", "moreThan6"])
                                .count()
                                .select(
                                    f.col("city_id"),
                                    f.col("profession_id"),
                                    f.col("dt"),
                                    f.col("noExperience").alias("no_experience_cnt"),
                                    f.col("between1And3").alias("between_1_and_3_cnt"),
                                    f.col("between3And6").alias("between_3_and_6_cnt"),
                                    f.col("moreThan6").alias("more_than_6_cnt")
                                ))
        logger.info("Trasform data vacancies to experience")

        salary = (vacancies.filter(f.col("salary_currency")=="RUR")
                    .groupBy(f.col("city_id"),
                                    f.col("profession_id"),
                                    f.col("dt")
                                    )
                    .pivot("experience",
                            ["noExperience", "between1And3", "between3And6", "moreThan6"])
                    .avg("salary")
                    .select(
                        f.col("city_id"),
                        f.col("profession_id"),
                        f.col("dt"),
                        f.round(f.col("noExperience"), 2).alias("no_experience_avg_salary"),
                        f.round(f.col("between1And3"), 2).alias("between_1_and_3_avg_salary"),
                        f.round(f.col("between3And6"), 2).alias("between_3_and_6_avg_salary"),
                        f.round(f.col("moreThan6"), 2).alias("more_than_6_avg_salary")
                        ))
        logger.info("Transform data vacancies to salary")

        schedule = (vacancies.groupBy(f.col("city_id")
                                        ,f.col("profession_id")
                                        ,f.col("dt")
                                        )
                    .pivot("schedule",["fullDay", "shift", "flexible", "remote", "flyInFlyOut"]) 
                    .count()
                    .select(f.col("city_id"),
                            f.col("profession_id"),
                            f.col("dt"),
                            f.col("fullDay").alias("full_day_schedule_cnt"),
                            f.col("shift").alias("shift_schedule_cnt"),
                            f.col("remote").alias("remote_schedule_cnt"),
                            f.col("flexible").alias("flexible_schedule_cnt"),
                            f.col("flyInFlyOut").alias("fly_in_fly_out_schedule_cnt")
                            ))
        logger.info("Transform data vacancies to shedule")

        metrics = (vacancies.groupBy(
            f.col("city_id"),
            f.col("profession_id"),
            f.col("dt")
            )
            .agg(f.count(f.col("vacancy_id")).alias("cnt"),
                    f.round(f.avg(f.col("salary")), 2).alias("avg_salary"))
            .select(
                f.col("city_id"),
                f.col("profession_id"),
                f.col("dt"),
                f.col("cnt"),
                f.col("avg_salary")))
        logger.info("Transform data vacancies to metrics")

        metrics = (
            metrics.join(experience, ["city_id", "profession_id"])
                .join(salary, ["city_id", "profession_id"])
                .join(schedule, ["city_id", "profession_id"])
                .select(
                    metrics["city_id"],
                    metrics["profession_id"],
                    metrics["dt"],
                    metrics["cnt"],
                    experience["no_experience_cnt"],
                    experience["between_1_and_3_cnt"],
                    experience["between_3_and_6_cnt"],
                    experience["more_than_6_cnt"],
                    metrics["avg_salary"],
                    salary["no_experience_avg_salary"],
                    salary["between_1_and_3_avg_salary"],
                    salary["between_3_and_6_avg_salary"],
                    salary["more_than_6_avg_salary"],
                    schedule["flexible_schedule_cnt"],
                    schedule["remote_schedule_cnt"],
                    schedule["full_day_schedule_cnt"],
                    schedule["shift_schedule_cnt"],
                    schedule["fly_in_fly_out_schedule_cnt"]
                ))
        
        logger.info("Return metrics")
        return metrics
    except:
        logger.error("Error return metrics")

def transform_to_skills_metrics(vacancies, profession_name, logger):
    try:
        vacancies = (vacancies.withColumn("lower_name", 
                        f.replace(f.lower(f.col('profession')), 
                                f.lit('-'), 
                                f.lit(" ")))
            .filter(((f.contains(f.col("lower_name"), 
                                f.lit(profession_name[0]))) | 
                    (f.contains(f.col("lower_name"), 
                                f.lit(profession_name[1])))) &
                                (f.col("dt_t").isNotNull())
                    )
            .withColumn("profession_id", f.lit(profession_name[2]))
            .select(
                f.col("vacancy_id"),
                f.col("city_id"),
                f.col("profession_id"),
                f.col("skills"),
                f.col("dt_t").alias("dt")))
        logger.info("Transform data vacancies to skills")

        skills = (vacancies.where(f.size(f.col("skills"))>0)
                    .select(f.col("city_id"), 
                            f.col("profession_id"),
                            f.col("dt"),
                            f.explode(f.col("skills")).alias("name"))
                    .groupBy(f.col("city_id"), 
                                f.col("profession_id"),
                                f.col("name"),
                                f.col("dt"))
                    .count()
                    .select(f.col("city_id"), 
                            f.col("profession_id"),
                            f.col("dt"), 
                            f.col("name"),
                            f.col("count").alias("cnt")
                            )
                    .orderBy(f.col('cnt').desc())
                    .limit(50)
                    )
        logger.info("Return skills")
        return skills
    except:
        logger.error("Error return skills")  

if __name__ == "__main__":  
    jar_postgres = os.environ['SPARK_HOME']
    spark = (SparkSession
         .builder
         .config('spark.jars', f'{jar_postgres}/jars/postgresql-42.6.0.jar')
         .master('local')
         .appName('ETL Statistic')
         .getOrCreate())
    context = spark.sparkContext
    logger = context._jvm.org.apache.log4j.LogManager.getLogger("com.contoso.PythonLoggerExample")
    logger.info("Spark session, spark context and logger created")
    config = get_config(logger)
    profession = get_table(spark, 'profession', config, logger)
    if profession:
        for profession_name in profession[["name", "en_name", "profession_id"]].collect():
            logger.info(f"Run with data profession - {profession_name}")

            vacancies = get_table(spark, 'vacancies', config, logger)
            metrics = transform_to_metrics(vacancies, profession_name, logger)
            # load_data(metrics, 'metrics', config, logger)
            skills = transform_to_skills_metrics(vacancies, profession_name, logger)
            skills.show(10)

            # if date.today().isoweekday()== 1:
            #     run_parse(profession_name[0], config, logger)
            #     vacancies = get_table(spark, 'vacancies', config, logger)
            #     metrics = transform_to_metrics(vacancies, profession_name, logger)
            #     load_data(metrics, 'metrics', config, logger)
            #     skills = transform_to_skills_metrics(vacancies, profession_name, logger)
            #     load_data(skills, 'skills', config, logger)
            #     logger.info(f"Job with {profession_name} successfully")
            # else:
            #     run_parse(profession_name[0], config, logger)
            #     logger.info(f"Job with {profession_name} successfully")

        logger.info("Spark job successfully")