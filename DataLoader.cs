using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Xml;
using System.Configuration;
using LogLibrary;

namespace dataloader
{
    public class DataLoader
    {
        private readonly Logger logger;
        
        public DataLoader(Logger logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void Run()
        {
            try
            {

                string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
                string tableName = ConfigurationManager.AppSettings["TableName"];
                string filePath = ConfigurationManager.AppSettings["FilePath"];


                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    connection.Open();

                    DataTable dataTable = ReadTable(connection, tableName);
                    DataTable tableSchema = GetTableSchema(connection, tableName);

                    string dataXmlFilePath = StoreDataInXml(dataTable, filePath);
                    string tableTypeXmlFilePath = StoreTableTypeInXml(tableSchema, filePath);
                    Console.WriteLine("Data successfully exported to XML.");


                    Logger.Log("Data successfully exported to XML.");
                }
            }
            catch (Exception ex)    
            {
                Logger.Log($"An error occurred: {ex.Message}");
                Console.WriteLine("An error occurred. Check logs for details.");
            }
        }

        private DataTable ReadTable(OdbcConnection connection, string tableName)
        {
            DataTable dataTable = new DataTable(tableName);
            using (OdbcCommand command = new OdbcCommand($"SELECT * FROM {tableName}", connection))
            {
                using (OdbcDataAdapter adapter = new OdbcDataAdapter(command))
                {
                    adapter.Fill(dataTable);
                }
            }
            Logger.Log("Table read succesfully");
            return dataTable;
        }

        private string StoreDataInXml(DataTable dataTable, string filePath)
        {
            string xmlFilePath = Path.Combine(filePath, "data.xml");
            dataTable.WriteXml(xmlFilePath);
            Console.WriteLine($"Table data information has been exported to {xmlFilePath}.");
            Logger.Log("Data Stored in XML File");
            return xmlFilePath;
        }
        private DataTable GetTableSchema(OdbcConnection connection, string tableName)
        {
            // Query to find constraints
            string constraintsQuery = $@"
        SELECT
            COLUMN_NAME,
           DATA_TYPE,
            IS_NULLABLE,
           CHARACTER_MAXIMUM_LENGTH,
            CASE
                WHEN COLUMN_NAME IN (
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = '{tableName}' AND CONSTRAINT_NAME LIKE 'PK_%'
               ) THEN 'Primary Key'
                WHEN COLUMN_NAME IN (
                   SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = '{tableName}' AND CONSTRAINT_NAME LIKE 'FK_%'
               ) THEN 'Foreign Key'
                ELSE 'Not a Primary Key'
            END AS CONSTRAINT_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tableName}'";

        //    // Query to find referenced table and columns
           string referencesQuery = $@"
        SELECT
            cp.name AS 'Column_Name',
            ref_tp.name AS 'Referenced_Table_Name',
            ref_cp.name AS 'Referenced_Column_Name'
        FROM 
            sys.foreign_keys AS fk
       INNER JOIN 
            sys.tables AS tp ON fk.parent_object_id = tp.object_id
        INNER JOIN 
            sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
        INNER JOIN 
            sys.columns AS cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
        INNER JOIN 
            sys.tables AS ref_tp ON fk.referenced_object_id = ref_tp.object_id
        INNER JOIN 
            sys.columns AS ref_cp ON fkc.referenced_column_id = ref_cp.column_id AND fkc.referenced_object_id = ref_cp.object_id
        WHERE tp.name = '{tableName}'";

            using (OdbcDataAdapter adapter = new OdbcDataAdapter(constraintsQuery, connection))
            {
                DataTable tableSchema = new DataTable(tableName);
                adapter.Fill(tableSchema);

                // Additional columns to store constraint information
                tableSchema.Columns.Add("IS_PRIMARY_KEY", typeof(bool));
                tableSchema.Columns.Add("IS_FOREIGN_KEY", typeof(bool));
                tableSchema.Columns.Add("FOREIGN_KEY_TABLE", typeof(string));
                tableSchema.Columns.Add("FOREIGN_KEY_COLUMN", typeof(string));

                // Update the additional columns based on CONSTRAINT_TYPE
                foreach (DataRow row in tableSchema.Rows)
                {
                    string columnName = row["COLUMN_NAME"].ToString();
                    string constraintType = row["CONSTRAINT_TYPE"].ToString();

                    switch (constraintType)
                    {
                        case "Primary Key":
                            row["IS_PRIMARY_KEY"] = true;
                            break;

                        case "Foreign Key":
                            row["IS_FOREIGN_KEY"] = true;
                            // Now, retrieve the referenced table and column using the second query
                            DataTable referencesTable = new DataTable();
                            using (OdbcDataAdapter referencesAdapter = new OdbcDataAdapter(referencesQuery, connection))
                            {
                                referencesAdapter.Fill(referencesTable);
                            }

                            if (referencesTable.Rows.Count > 0)
                            {
                                row["FOREIGN_KEY_TABLE"] = referencesTable.Rows[0]["Referenced_Table_Name"].ToString();
                                row["FOREIGN_KEY_COLUMN"] = referencesTable.Rows[0]["Referenced_Column_Name"].ToString();
                            }
                            break;

                        default:
                            row["IS_FOREIGN_KEY"] = false;
                            break;
                    }
                }

                // Remove the CONSTRAINT_TYPE column
                tableSchema.Columns.Remove("CONSTRAINT_TYPE");

                return tableSchema;
            }
        }




        private string StoreTableTypeInXml(DataTable tableSchema, string filePath)
        {
            string tableTypeXmlFilePath = Path.Combine(filePath, "tableType1.xml");
            tableSchema.WriteXml(tableTypeXmlFilePath);


            Console.WriteLine($"Table type information has been exported to {tableTypeXmlFilePath}.");
            Logger.Log("Schema Stored in XML");
            return tableTypeXmlFilePath;
        }

    }
}
