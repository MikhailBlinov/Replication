using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using TableSerializationLib;

namespace ReplicationProject
{
    public class Source
    {
        public Source(string sourceConnectionString, string sourceTableName)
        {
            _connectionString = sourceConnectionString;
            _sourceTableName = sourceTableName;
            _tableData = new TableData();
        }

        // Строка соединения с источником
        private readonly string _connectionString;
        private string _sourceTableName;

        private DataTable _sourceTable;

        // Коллеция метаданных
        private List<MetaData> _metaData; 
        private TableData _tableData;

        // Функция получения метаданных
        private void GetMetaData()
        {
            _metaData = new List<MetaData>();

            string selectString = @" SELECT COLUMN_NAME,
                                     DATA_TYPE FROM 
                                     INFORMATION_SCHEMA.COLUMNS 
                                     WHERE TABLE_NAME = " + "'" + _sourceTableName + "'";

            
                SqlDataReader reader = null;

                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(selectString, connection))
                    {
                        using (reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MetaData metaDataDateType = new MetaData();
                                metaDataDateType.Name = (string) reader["COLUMN_NAME"];
                                metaDataDateType.Type = (string) reader["DATA_TYPE"];

                            _metaData.Add(metaDataDateType);
                            }
                        }
                    }
                }
        }


        // Функция получения запроса к источнику
         private string CreateSelectQuery()
        {
            string columnList = string.Empty;

            for (int i = 0; i < _metaData.Count; i++)
            {
                if (i < _metaData.Count - 1)
                {
                    columnList = columnList + _metaData[i].Name + ",";
                }
                else
                {
                    columnList = columnList + _metaData[i].Name;
                }
            }

            return @" SELECT " + columnList + " FROM " + _sourceTableName;
        }

        private void FillSourceTable()
        {
            string selectString = CreateSelectQuery();

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                using (SqlCommand command = new SqlCommand(selectString))
                {
                    command.Connection = connection;

                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        this._sourceTable = new DataTable();
                        adapter.Fill(this._sourceTable);
                    }
                }
            }
        }

        private byte[] GetSeriolizedTableDataBytes()
        {
            byte[] bytes = null;

            BinaryFormatter formatter = new BinaryFormatter();

            using (MemoryStream memoryStream = new MemoryStream())
            {
                formatter.Serialize(memoryStream, _tableData);

                bytes = memoryStream.ToArray();
            }

            return bytes;

        }

        public byte[] PrepareTableData()
        {
            byte[] result = null;

                GetMetaData();
                FillSourceTable();

                _tableData.TableName = _sourceTableName;
                _tableData.MetaData = _metaData;
                _tableData.Table = _sourceTable;

                result = GetSeriolizedTableDataBytes();

            return result;
        }
    }
}
