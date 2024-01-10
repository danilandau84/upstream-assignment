import bonus
import bronze
import silver
import gold
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    bronze.run()
    silver.run()
    gold.run()
    bonus.generate_sql_injection_report(["vin", "manufacturer"], ["(SELECT|ALTER|EXEC|EXE|EXECUTE|DELETE|TABLE)"])
