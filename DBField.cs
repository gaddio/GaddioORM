using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gaddio.ORM
{
    public class DBField : Attribute
    {
        [Flags]
        public enum DBFieldKeys : byte
        {
            None = 0,
            Primary = 1,
            Insertable = 2,
            AutoIncrement = 4,
            Nullable = 8,
            DeletableBy = 16,
            SelectableBy = 32
        }

        public DBField(string fieldName, DBFieldKeys key)
        {
            this.fieldName = fieldName;
            this.key = key;
        }

        public DBField(string fieldName)
        {
            this.fieldName = fieldName;
            this.key = DBFieldKeys.None;
        }

        public DBField()
        {

        }

        private DBFieldKeys key = DBFieldKeys.None;

        public DBFieldKeys Key
        {
            get { return key; }
            set { key = value; }
        }

        private string fieldName = "";

        public string FieldName
        {
            get { return fieldName; }
            set { fieldName = value; }
        }
    }
}
