using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableSerializationLib
{
    [Serializable]
    public class TableData
    {
        public string TableName { get; set; }
        public List<MetaData> MetaData { get; set; }
        public DataTable Table { get; set; }
    }
}
