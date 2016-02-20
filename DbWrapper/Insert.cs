using DbWrapper.Read;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Text;

namespace DbWrapper
{
    public class Insert : Command
    {
        /*
         * Private class variables
         */
        private Record[] _records;
        private Record _record;

        /*
         * Class constructors
         */
        public Insert(Record[] recs) : base() 
        {
            _records = recs;
        }

        public Insert(Record rec, string table, Database db) : base(db)
        {
            _record = rec;
            _records = new Record[]{rec};
            _table = table;
        }

        /*
         * Public override methods
         */
        public override string ToString()
        {
            CreateCommand();
            return base.ToString();
        }

        /// <summary>
        /// Creates the SQL INSERT command based on the list of records.
        /// </summary>
        public override void CreateCommand()
        {
            base.CreateCommand();

            CreateInsertSection();
            _command.CommandText = _commandStr.ToString();
        }

        /*
         * Private class methods
         */
        /// <summary>
        /// Generates the INSERT section of the SQL statement.
        /// </summary>
        private void CreateInsertSection()
        {
            _commandStr.Append(String.Format("INSERT INTO [{0}] ( ", _table));

            /*
             * Loops through the property set of the first record
             * in order to get the names of the columns to be inserting to
             */
            Hashtable props = _records[0].PropertyList;

            short counter = 0;
            foreach (var key in props.Keys)
            {
                int fkIndex = _record.IdentityColumns.FindIndex(x =>
                {
                    if (x.IsPrimaryKey == false &&
                        x.Column.Equals(key))
                        return true;
                    else
                        return false;
                });
                int pkIndex = _record.IdentityColumns.FindIndex(x =>
                {
                    if (x.IsPrimaryKey == true &&
                        x.Column.Equals(key))
                        return true;
                    else
                        return false;
                });

                if (pkIndex == -1)
                {
                    if (counter != props.Count - 1)
                    {
                        _commandStr.Append(String.Format("{0}\n\t, ", key));
                    }
                    else
                    {
                        _commandStr.Append(String.Format("{0})\n", key));
                    }
                }
                counter++;
            }
            // props = null;
            _commandStr.Append("VALUES ( ");

            /*
             * This is an exausting process so we'll explain it step by step.
             * First I want to loop through every record found in _records.
             */
            for (short i = 0; i < _records.Length; i++)
            {
                List<OdbcParameter> paramList = new List<OdbcParameter>();
                props = _records[i].PropertyList;

                /*
                 * Now that I have the list of properties for _records[i], I
                 * loop through each record's properties to build a parameter
                 * for each one.
                 */
                short num = 0;
                foreach (var key in props.Keys)
                {
                    int fkIndex = _record.IdentityColumns.FindIndex(x => 
                    {
                        if (x.IsPrimaryKey == false &&
                            x.Column.Equals(key))
                            return true;
                        else
                            return false;
                    });
                    int pkIndex = _record.IdentityColumns.FindIndex(x => 
                    {
                        if (x.IsPrimaryKey == true &&
                            x.Column.Equals(key))
                            return true;
                        else
                            return false;
                    });

                    if (pkIndex == -1)
                    {
                        object val = props[key];
                        string column = key.ToString();
                        WhereClause clause = new WhereClause(String.Format("{0}/=/{1}", column, val));
                        SetClauseDataType(ref clause, key);
                        OdbcParameter param = BuildParam(ref clause,
                                                        "I" + Convert.ToString(num));
                        paramList.Add(param);
                    }
                    num++;
                }

                int paramCount = paramList.Count;

                /*
                 * I have my parameters added now.  I'm going to now loop through
                 * each parameter for _records[i] to build the list of VALUES()
                 * for the SQL statement.
                 */
                for (int j = 0; j < paramCount; j++)
                {
                    // If this isn't the last parameter
                    if (j != paramCount - 1)
                    {
                        _commandStr.Append(String.Format("{0}\n\t, ",
                                                    paramList[j].ParameterName));
                    }
                    else // Now we are dealing with the last parameter
                    {
                        _commandStr.Append(String.Format("{0})",
                                                    paramList[j].ParameterName));
                    }
                }

                // If there are more records, start the next value group
                if (i != _records.Length - 1)
                    _commandStr.Append(",\n\t(");

                for (short k = 0; k < paramList.Count; k++)
                    _command.Parameters.Add(paramList[k]);

                paramList.Clear();
                paramList = null;
            }
        }
        private void SetClauseDataType(ref WhereClause clause, object key)
        {
            if (_record.PropertyListInfo[key] != null)
            {
                clause.DataType = (Type)_record.PropertyListInfo[key];

                if ((Type)_record.PropertyListInfo[key] == typeof(byte[]))
                {
                    if (_record.PropertyList[key] != DBNull.Value)
                        clause.Value = (byte[])_record.PropertyList[key];
                }
            }
            else
                clause.DataType = (Type)_record.PropertyListInfo[key];
        }
    }
}