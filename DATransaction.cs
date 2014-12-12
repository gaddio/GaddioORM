using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Gaddio.ORM;
using System.Data.SqlClient;
using System.Security.Principal;

namespace Gaddio.ORM
{
    public class DATransaction : IDisposable
    {
        public event EventHandler TransactionCommitted;
        public event EventHandler TransactionRolledBack;

        private SqlConnection conn = null;
        private SqlTransaction tx = null;
        private WindowsImpersonationContext impersonationContext = null;
        private bool committed = false;

        private DATransaction(SqlConnection conn)
        {
            this.conn = conn;
        }

        public static DATransaction BeginProcessContextTransaction()
        {
            SqlConnection conn = DbConnection.Get();
            DATransaction dtx = new DATransaction(conn);
            dtx.impersonationContext = DataAccessUtil.RevertToProcessContext();
            conn.Open();
            dtx.tx = dtx.conn.BeginTransaction();

            return dtx;
        }

        public static DATransaction BeginImpersonationContextTransaction(SqlConnection conn)
        {
            DATransaction dtx = new DATransaction(conn);
            dtx.tx = dtx.conn.BeginTransaction();

            return dtx;
        }

        public SqlTransaction Tx
        {
            get { return tx; }
            set { tx = value; }
        }

        public void Commit()
        {
            this.tx.Commit();

            this.committed = true;

            if (this.TransactionCommitted != null)
            {
                this.TransactionCommitted(this, EventArgs.Empty);
            }
        }

        public void Rollback()
        {
            this.tx.Rollback();

            if (this.TransactionRolledBack != null)
            {
                this.TransactionRolledBack(this, EventArgs.Empty);
            }
        }

        public bool Committed
        {
            get { return committed; }
            set { committed = value; }
        }

        public void Dispose()
        {
            if (this.tx != null)
            {
                this.tx.Dispose();
            }

            if (this.conn != null)
            {
                try
                {
                    this.conn.Close();
                }
                catch (Exception) { }
            }

            if (this.impersonationContext != null)
            {
                DataAccessUtil.ReturnToImpersonationContext(this.impersonationContext);
            }
        }
    }
}
