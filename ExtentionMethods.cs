using System;
using System.IO;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;

namespace DatabaseAccess
{
    /// <summary>
    /// Extention Method Library
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Add parameters to DbCommand object
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="values"></param>
        public static void AddParametersToQuery(this DbCommand cmd, object[] values)
        {
            if (values.IsNullOrEmpty() == false)
            {
                for (int i = 0; i < values.Count(); i += 2)
                {
                    string paramName = values[i].ToString();

                    if (values[i + 1] is string && (string)values[i + 1] == "")
                        values[i + 1] = null;

                    // If value is null then give it a value equal DBNull.Value
                    // else assign the indexed value.
                    object value = values[i + 1] ?? DBNull.Value;

                    var dbParam = cmd.CreateParameter();
                    dbParam.ParameterName = paramName;
                    dbParam.Value = value;

                    cmd.Parameters.Add(dbParam);
                }
            }
        }

        /// <summary>
        /// Retrieve table schema
        /// </summary>
        /// <param name="dr"></param>
        /// <returns></returns>
        public static List<string> GetTableColumnNames(IDataReader dr)
        {
            List<string> columns = new List<string>();

            try
            {
                DataTable schemaTable = dr.GetSchemaTable();

                for (int i = 0; i < schemaTable.Rows.Count; i++)
                {
                    columns.Add(schemaTable.Rows[i].ItemArray[0].ToString());
                }
            }
            catch (Exception exc)
            { throw exc; }

            return columns;
        }

        /// <summary>
        /// Table schema - Column data types
        /// </summary>
        /// <param name="dr"></param>
        /// <returns></returns>
        public static Dictionary<string, string> GetTableColumnTypes(IDataReader dr)
        {
            List<string> types = new List<string>();

            Dictionary<string, string> returnDic = new Dictionary<string, string>();

            try
            {
                DataTable schemaTable = dr.GetSchemaTable();

                for (int i = 0; i < schemaTable.Rows.Count; i++)
                {
                    returnDic.Add(schemaTable.Rows[i].ItemArray[0].ToString(), schemaTable.Rows[i].ItemArray[12].ToString());
                }
            }
            catch (Exception exc)
            {
                string errorMessage = exc.Message;
                return new Dictionary<string, string>();
            }

            return returnDic;
        }

        /// <summary>
        /// Convert object into int
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int ToInt(this object value)
        {
            int result = 0;

            if (value.IsNullOrEmpty())
            { return -1; }

            bool isInt = int.TryParse(value.ToString(), out result);

            if (isInt) { return result; }
            else { return -1; }
        }

        /// <summary>
        /// Convert object to DateTime
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static DateTime ToDateTime(this object value)
        {
            DateTime result;
            bool isDateTime;

            if (value.IsNullOrEmpty() == false) { isDateTime = DateTime.TryParse(value.ToString(), out result); }
            else { return new DateTime(); }

            if (isDateTime) { return result; }
            else { return new DateTime(); }
        }

        /// <summary>
        /// Convert object to decimal
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static decimal ToDecimal(this object value)
        {
            decimal result;
            bool isDecimal;

            if (value.IsNullOrEmpty() == false)
            { isDecimal = decimal.TryParse(value.ToString(), out result); }
            else { return -1; }

            if (isDecimal) { return result; }
            else { return -1; }
        }

        /// <summary>
        /// Convert object to decimal
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static double ToDouble(this object value)
        {
            double result;
            bool isDouble;

            if (value.IsNullOrEmpty() == false)
                isDouble = double.TryParse(value.ToString(), out result);
            else
                return -1;

            if (isDouble) { return result; }
            else { return -1.0; }
        }

        /// <summary>
        /// Check object array is null or contains no elements
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public static bool IsNullOrEmpty(this object[] values)
        {
            if (values == null)
            {
                return true;
            }
            else if (values.Count() == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Check single object is null or contains no elements
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool IsNullOrEmpty(this object value)
        {
            if (value == null)
            {
                return true;
            }
            else if (value.ToString().Length == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
