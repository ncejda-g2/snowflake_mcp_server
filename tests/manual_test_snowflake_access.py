import snowflake.connector

# Test with explicit values
conn = snowflake.connector.connect(
    account="GJA24605-DATAWAREHOUSE",
    user="NCEJDA@G2.COM",  # The user who created the PAT
    authenticator="externalbrowser",
    warehouse="ML_DEV_WH",
    database="ML_DEV",
    role="ML_DEVELOPER",  # Add the role your PAT has access to
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
print(cursor.fetchone())
