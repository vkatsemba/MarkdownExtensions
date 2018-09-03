setwd("C:\\Users\\Vadim_Katsemba\\Documents")

library(RJDBC)
library(DBI)

driver <- JDBC(driverClass = "org.netezza.Driver", classPath = "C:\\Users\\Vadim_Katsemba\\Downloads\\nzjdbc.jar", "'")
conn <- dbConnect(driver, "jdbc:netezza://prd1905.cs.ctc:5480/EDWPRD", "RpaBot1", "QazPlm1$")


q_df <- dbSendQuery(conn, "SELECT 
                    DEAL_CURRENT_DIM.DEAL_ID, 
                    DEAL_CURRENT_DIM.DEAL_YEAR, 
                    DEAL_CURRENT_DIM.DEAL_NUM, 
                    DEAL_CURRENT_DIM.DEAL_NM, 
                    DEAL_PRODUCT_CURRENT_DIM.DEAL_FLYER_PAGE_NUM, 
                    DEAL_CURRENT_DIM.DEAL_START_DATE, 
                    DEAL_CURRENT_DIM.DEAL_END_DATE, 
                    DEAL_PRODUCT_CURRENT_DIM.ACTIVE_IND, 
                    PRODUCT_CTR_CURRENT_DIM.DIVISION_NM, 
                    PRODUCT_CTR_CURRENT_DIM.LOB_NM, 
                    PRODUCT_CTR_CURRENT_DIM.CATEGORY_NM, 
                    PRODUCT_CTR_CURRENT_DIM.SUBCATEGORY_NM, 
                    PRODUCT_CTR_CURRENT_DIM.FINELINE_NM, 
                    PRODUCT_CTR_CURRENT_DIM.PRODUCT_NUM, 
                    PRODUCT_CTR_CURRENT_DIM.PRODUCT_ENGLISH_DESC, 
                    PRODUCT_CTR_CURRENT_DIM.CORPORATE_STATUS_CD, 
                    PRODUCT_CTR_CURRENT_DIM.CORPORATE_STATUS_CD_CHANGE_DATE, 
                    PRODUCT_CTR_CURRENT_DIM.DEALER_RESTRICTION_CD, 
                    PRODUCT_CTR_CURRENT_DIM.NATIONAL_DEALER_PRICE_AMT, 
                    PRODUCT_CTR_CURRENT_DIM.NATIONAL_CONSUMER_PRICE_AMT
                    FROM (DEAL_CURRENT_DIM INNER JOIN DEAL_PRODUCT_CURRENT_DIM ON DEAL_CURRENT_DIM.DEAL_ID = DEAL_PRODUCT_CURRENT_DIM.DEAL_ID) INNER JOIN PRODUCT_CTR_CURRENT_DIM ON DEAL_PRODUCT_CURRENT_DIM.PRODUCT_ID = PRODUCT_CTR_CURRENT_DIM.PRODUCT_ID
                    WHERE (((DEAL_CURRENT_DIM.DEAL_NUM) <> '96%') AND ((DEAL_CURRENT_DIM.DEAL_END_DATE)='09/15/2018'))")

q_fet <- dbFetch(q_df, n = -1)
write.csv(q_fet, "Query1Results.csv")