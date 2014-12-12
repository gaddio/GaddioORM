using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gaddio.ORM
{
    public class DBTable : Attribute
    {
        public enum DBTableSources : byte
        {
            None = 0,
            Table = 1,
            StoredProcedure = 2,
            View = 3
        }

        private DBTableSources source = DBTableSources.Table;

        public DBTableSources Source
        {
            get { return source; }
        }

        private bool deletable;

        public bool Deletable
        {
            get { return deletable; }
        }

        private bool insertable;

        public bool Insertable
        {
            get { return insertable; }
        }

        private bool updateable;

        public bool Updateable
        {
            get { return updateable; }
        }
        private string tableName = null;

        public string TableName
        {
            get { return tableName; }
        }

        public DBTable(bool insertable, bool updateable, bool deletable)
        {
            this.deletable = deletable;
            this.updateable = updateable;
            this.insertable = insertable;
        }

        public DBTable(string tableName, bool insertable, bool updateable, bool deletable)
        {
            this.tableName = tableName;
            this.deletable = deletable;
            this.updateable = updateable;
            this.insertable = insertable;
        }

        public DBTable(string tableName, bool insertable, bool updateable, bool deletable, DBTableSources source)
        {
            this.tableName = tableName;
            this.source = source;
            this.deletable = deletable;
            this.updateable = updateable;
            this.insertable = insertable;
        }
    }
}
