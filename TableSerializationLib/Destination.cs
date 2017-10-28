using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TableSerializationLib;

namespace ReplicationProject
{
    public class Destination
    {
        private string _connectionString;
        private List<MetaData> _metaData;
        public string _destinationTableName;
        private DataTable _destinationTable;
        private TableData _tableData;

        public Destination(string connectionString)
        {
            _connectionString = connectionString;
        }
            // Функция получения запроса к целевой таблице
             string CreateInsertQuery()
            {
                string columnList = string.Empty;
                string parameterList = string.Empty;

                for (int i = 0; i < _metaData.Count; i++)
                {
                    if (i < _metaData.Count - 1)
                    {
                        columnList = columnList + _metaData[i].Name + ",";
                        parameterList = parameterList + "@" + _metaData[i].Name + ",";
                    }
                    else
                    {
                        columnList = columnList + _metaData[i].Name;
                        parameterList = parameterList + "@" + _metaData[i].Name;
                    }
                }

                return @"INSERT INTO " + _destinationTableName + "(" + columnList + ")" + @"  VALUES " + "(" + parameterList + ")";
            }
      
        // Фукнция получения параметра для команды
         private SqlParameter GetParameter(MetaData metaDate, object value)
        {
            SqlParameter parameter = new SqlParameter();
            parameter.ParameterName = metaDate.Name;

            switch (metaDate.Type)
            {
                case "money":
                case "smallmoney":
                    parameter.DbType = DbType.Decimal;
                    parameter.Value = (DateTime)value;
                    break;

                case "date":
                    parameter.DbType = DbType.Date;
                    parameter.Value = (DateTime)value;
                    break;

                case "datetimeoffset":
                    parameter.DbType = DbType.DateTimeOffset;
                    parameter.Value = (DateTime)value;
                    break;

                case "datetime2":
                    parameter.DbType = DbType.DateTime2;
                    parameter.Value = (DateTime)value;
                    break;

                case "smalldatetime":
                    parameter.DbType = DbType.DateTime;
                    parameter.Value = (DateTime)value;
                    break;

                case "datetime":
                    parameter.DbType = DbType.DateTime;
                    parameter.Value = (DateTime)value;
                    break;

                case "time":
                    parameter.DbType = DbType.Time;
                    parameter.Value = (DateTime)value;
                    break;

                case "char":
                case "varchar":
                case "nvarchar":
                case "text":
                    parameter.DbType = DbType.String;
                    parameter.Value = (string)value;
                    break;

                case "real":
                    parameter.DbType = DbType.Single;
                    parameter.Value = (float)value;
                    break;

                case "float":
                    parameter.DbType = DbType.Double;
                    parameter.Value = (Int16)value;
                    break;

                case "tinyint":
                    parameter.DbType = DbType.Int16;
                    parameter.Value = (Int16)value;
                    break;

                case "bit":
                    parameter.DbType = DbType.Boolean;
                    parameter.Value = (bool)value;
                    break;

                case "decimal":
                    parameter.DbType = DbType.Decimal;
                    parameter.Value = (decimal)value;
                    break;

                case "numeric":
                    parameter.DbType = DbType.Decimal;
                    parameter.Value = (decimal)value;
                    break;

                case "bigint":
                    parameter.DbType = DbType.Int64;
                    parameter.Value = (long)value;
                    break;

                case "int":
                    parameter.DbType = DbType.Int32;
                    parameter.Value = (int)value;
                    break;

                case "string":
                    parameter.DbType = DbType.String;
                    parameter.Value = (string)value;
                    break;
            }
            //smallmoney
            //money
            return parameter;
        }

        public void InsertDestinationData()
        {
            using (SqlConnection connectionInsertConnection = new SqlConnection(_connectionString))
            {
                connectionInsertConnection.Open();

                string query = CreateInsertQuery();

                using (SqlCommand insertCommand = new SqlCommand(query, connectionInsertConnection))
                {
                    foreach (DataRow row in _tableData.Table.Rows)
                    {
                        // Для каждого элемента DataReader формируем параметры , помещаем параметры в объект command и выполняем зпись в таблицу
                        foreach (MetaData metaData in _tableData.MetaData)
                        {
                            SqlParameter parameter = GetParameter(metaData, row[metaData.Name]);
                            insertCommand.Parameters.Add(parameter);
                        }

                        insertCommand.ExecuteNonQuery();
                        insertCommand.Parameters.Clear();
                    }
                }
            }
        }

        public void Deserialize(byte[] bts)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                memoryStream.Write(bts, 0, bts.Count());
                memoryStream.Position = 0;

                BinaryFormatter binaryFormatter = new BinaryFormatter();
                _tableData = (TableData)binaryFormatter.Deserialize(memoryStream);
            }
        }


        public void PerformReplication(byte[] bytes)
        {
            Deserialize(bytes);

            // TODO в данном случае предполагается , что исходная таблица и таблица назначения одинаковы. 
            // при реализации это нужно изменить в пользу построения метаданных и наименования таблицы назначения
            //_destinationTableName = _tableData.TableName;
            _destinationTableName = "TestTableTo";
            _metaData = _tableData.MetaData;
            _destinationTable = _tableData.Table; 

            InsertDestinationData();
        }
    }
}
