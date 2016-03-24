using System;
using System.IO;
using System.Data;
using System.Data.Common;
using System.Data.ProviderBase;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Configuration;

namespace DatabaseAccess
{
    /// <summary>
    /// Data Access Class - Allows retrieval and insertion of data into any type of database supported
    /// by the .Net Framework. Simply provide instance of the class with the connection string of the database
    /// you need to work with and the Database Factory Provider (SQL, Access, Postgres, SQLite etc.)
    /// </summary>
    public class DatabaseWorker : IDisposable
    {
        #region Private Declarations

        private string _connectionString;
        private string _factoryProvider;
        private string _latestError;

        // This is the default value for the 'factory provider'
        private const string DefaultFactoryProvider = "System.Data.SqlClient";

        private DbProviderFactory _dataBaseProvider;

        #endregion

        #region Properties

        /// <summary>
        /// Database ConnectionString
        /// </summary>
        public string ConnectionString
        {
            get { return _connectionString; }
            set { _connectionString = value; }
        }

        /// <summary>
        /// The provider for the Database
        /// </summary>
        public string FactoryProvider
        {
            get
            {
                if (string.IsNullOrEmpty(_factoryProvider))
                {
                    return DefaultFactoryProvider;
                }
                else
                {
                    return _factoryProvider;
                }
            }
            set { _factoryProvider = value; }
        }

        /// <summary>
        /// Last error message set by class
        /// </summary>
        public string ErrorMessage
        {
            get { return _latestError; }
            private set { _latestError = value; }
        }

        #endregion

        #region Constructor(s)

        /// <summary>
        /// Default contructor
        /// </summary>
        public DatabaseWorker()
        {
            _dataBaseProvider = DbProviderFactories.GetFactory(FactoryProvider);
        }

        /// <summary>
        /// Override Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="factoryProvider"></param>
        public DatabaseWorker(string connectionString, string factoryProvider)
        {
            if (connectionString != "") { _connectionString = connectionString; }
            else { _latestError = "No connection string given"; }

            _factoryProvider = factoryProvider;

            // This will be used to create all connections anc all commands for querying
            _dataBaseProvider = DbProviderFactories.GetFactory(FactoryProvider);
        }

        #endregion

        /// <summary>
        /// Open a connection to the specified database. returns true if connected else returns false
        /// </summary>
        /// <returns></returns>
        public bool TestConnection()
        {
            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();
                }

                return true;
            }
            catch (Exception exc)
            {
                _latestError = exc.Message;
                return false;
            }
        }

        /// <summary>
        /// Retrieve a list of all tables in database and matchgiven string
        /// to the table names. If match made then returns true else false        
        /// </summary>
        /// <returns></returns>
        public bool TableExists(string tableName)
        {
            List<string> allTables = new List<string>();

            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    DataTable schema = connection.GetSchema("Tables");

                    // Loop through returned values until we find the argument value
                    foreach (DataRow row in schema.Rows)
                    {
                        if (row[2].ToString() == tableName)
                        {
                            return true;
                        }
                    }

                    return false;
                }
            }
            catch (Exception exc)
            {
                _latestError = exc.Message;
                return false;
            }
        }

        /// <summary>
        /// Returns a DataTable object populated with results from database query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public DataTable ToDataTable(string query, object[] queryParams)
        {
            DataTable resultsTable = new DataTable();

            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {
                    if (connection != null)
                    {
                        connection.ConnectionString = ConnectionString;
                        connection.Open();
                        using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                        {
                            if (cmd != null)
                            {
                                cmd.CommandText = query;
                                cmd.Connection = connection;
                                cmd.AddParametersToQuery(queryParams);

                                IDataReader dr = cmd.ExecuteReader();
                                resultsTable.Load(dr);
                            }
                        }
                    }
                }

                return resultsTable;
            }
            catch (DbException exc)
            {
                _latestError = exc.Message;
                return new DataTable();
            }
        }

        /// <summary>
        /// Returns a List of object's of type T populated with results from database query.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public List<T> ToList<T>(string query, object[] queryParams)
        {
            // List that will be returned to method called
            List<T> returnList = new List<T>();

            T newInstance = default(T);
            List<string> tableColumns = null;

            Type type = typeof(T);

            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {

                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = query;
                        cmd.AddParametersToQuery(queryParams);

                        IDataReader dr = cmd.ExecuteReader();

                        while (dr.Read())
                        {
                            // Creates a new instance of the object supplied to the method
                            newInstance = Activator.CreateInstance<T>();

                            PropertyInfo[] propInfo = newInstance.GetType().GetProperties();

                            if (tableColumns == null)
                                tableColumns = ExtensionMethods.GetTableColumnNames(dr);

                            for (int i = 0; i < propInfo.Length; i++)
                            {
                                if (tableColumns.Contains(propInfo[i].Name) && !object.Equals(dr[propInfo[i].Name], DBNull.Value))
                                {
                                    Type instanceType = newInstance.GetType();

                                    // Set value of property by invoking the specified member of the instance type
                                    instanceType.InvokeMember(propInfo[i].Name, BindingFlags.SetProperty, null, newInstance, new object[] { dr[propInfo[i].Name] });
                                }
                            }

                            returnList.Add(newInstance);
                        }
                    }
                }

                return returnList;
            }
            catch (Exception exc)
            {
                _latestError = exc.Message;
                return new List<T>();
            }
        }

        /// <summary>
        /// Get the schema for the relevant table and returns a collection of strings with the column names within
        /// </summary>
        /// <param name="tableName">Name of table</param>
        /// <returns></returns>
        public List<string> GetTableColumnNames(string tableName)
        {
            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {

                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = string.Format("SELECT * FROM {0}", tableName);

                        IDataReader dr = cmd.ExecuteReader();

                        List<string> tableColumns = null;

                        if (tableColumns == null)
                            tableColumns = ExtensionMethods.GetTableColumnNames(dr);

                        return tableColumns;
                    }
                }
            }
            catch (Exception exc)
            {
                _latestError = exc.Message;
                return null;
            }
        }

        /// <summary>
        /// Get the data types of each column in table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetColumnDataTypes(string tableName)
        {
            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {

                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = string.Format("SELECT * FROM {0}", tableName);

                        IDataReader dr = cmd.ExecuteReader();
                        Dictionary<string, string> tempDic = null;

                        if (tempDic == null)
                            tempDic = ExtensionMethods.GetTableColumnTypes(dr);

                        return tempDic;
                    }
                }
            }
            catch (Exception exc)
            {
                _latestError = exc.Message;
                return null;
            }
        }

        /// <summary>
        /// Return an object of type T populated with result of database query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public T ReadOne<T>(string query, object[] queryParams)
        {
            T newInstance = default(T);
            List<string> tableColumns = null;

            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                    {
                        cmd.Connection = connection;
                        cmd.CommandText = query;
                        cmd.AddParametersToQuery(queryParams);

                        IDataReader dr = cmd.ExecuteReader();

                        // Asks the Datareader to read the next item as we only want one
                        if (dr.Read())
                        {
                            // Creates a new instance of the object supplied to the method
                            newInstance = Activator.CreateInstance<T>();

                            PropertyInfo[] propInfo = newInstance.GetType().GetProperties();

                            if (tableColumns == null)
                                tableColumns = ExtensionMethods.GetTableColumnNames(dr);

                            for (int i = 0; i < propInfo.Length; i++)
                            {
                                if (tableColumns.Contains(propInfo[i].Name) && object.Equals(dr[propInfo[i].Name], DBNull.Value) == false)
                                {
                                    Type instanceType = newInstance.GetType();

                                    // Set value of property by invoking the specified member of the instance type
                                    if (dr[propInfo[i].Name].IsNullOrEmpty() == false)
                                        instanceType.InvokeMember(propInfo[i].Name, BindingFlags.SetProperty, null, newInstance, new object[] { dr[propInfo[i].Name] });
                                }
                            }
                        }
                    }

                    return newInstance;
                }
            }
            catch (DbException exc)
            {
                _latestError = exc.Message;
                return default(T);
            }
        }

        /// <summary>
        /// Return first column of first row of query result
        /// </summary>
        /// <param name="query"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public object ReadScalar(string query, object[] queryParams)
        {
            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                    {
                        cmd.CommandText = query;
                        cmd.Connection = connection;
                        cmd.AddParametersToQuery(queryParams);

                        return cmd.ExecuteScalar();
                    }
                }
            }
            catch (DbException exc)
            {
                _latestError = exc.Message;
                return new object();
            }
        }

        /// <summary>
        /// Query template for Insert, Delete and Update query methods.
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        private bool QueryTable(string sqlQuery, object[] queryParams)
        {
            try
            {
                using (var connection = _dataBaseProvider.CreateConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    using (var cmd = _dataBaseProvider.CreateCommand())
                    {
                        cmd.CommandText = sqlQuery;
                        cmd.Connection = connection;
                        cmd.AddParametersToQuery(queryParams);

                        return cmd.ExecuteNonQuery() > 0;
                    }
                }
            }
            catch (DbException dbExc)
            {
                _latestError = dbExc.Message;
                return false;
            }
        }

        /// <summary>
        /// Returns true if Insert query was successful
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public bool InsertToTable(string sqlQuery, object[] queryParams)
        {
            return QueryTable(sqlQuery, queryParams);
        }

        /// <summary>
        /// Returns true if Update query was successful
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public bool UpdateTable(string sqlQuery, object[] queryParams)
        {
            return QueryTable(sqlQuery, queryParams);
        }

        /// <summary>
        /// Returns true if Delete query was successful
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <param name="queryParams"></param>
        /// <returns></returns>
        public bool DeleteFromTable(string sqlQuery, object[] queryParams)
        {
            return QueryTable(sqlQuery, queryParams);
        }

        /// <summary>
        /// Returns true if alter table statement successfull
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <param name="parms"></param>
        /// <returns></returns>
        public bool AlterTable(string sqlQuery, object[] parms)
        {
            return QueryTable(sqlQuery, parms);
        }

        /// <summary>
        /// SQL Transaction, this is rolled back after it has been executed
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <returns></returns>
        public int PerformTransactionWithRollBack(string sqlQuery)
        {
            try
            {
                using (DbConnection connection = _dataBaseProvider.CreateConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();

                    DbTransaction transaction = connection.BeginTransaction();

                    using (DbCommand cmd = _dataBaseProvider.CreateCommand())
                    {
                        /* First command of the transaction */
                        cmd.Connection = connection;
                        cmd.Transaction = transaction;
                        cmd.CommandText = sqlQuery;

                        int rowsAffeted = cmd.ExecuteNonQuery();

                        /*  Attempt To Rollback the transaction */
                        try
                        {
                            transaction.Rollback();
                        }
                        catch (Exception exc)
                        {
                            _latestError = exc.Message;
                        }

                        return rowsAffeted;
                    }
                }
            }
            catch (Exception exc)
            {
                _latestError = exc.Message;
                return -1;
            }
        }

        #region IDisposable Implementation

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.ConnectionString = "";
                this.ErrorMessage = "";
                this.FactoryProvider = "";
                _dataBaseProvider = null;
            }
        }

        #endregion
    }
}
