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
    public class Update : Command
    {
        /*
         * Private class variables
         */
        private Record _record;

        /*
         * Class constructors
         */
        public Update(Record rec, string table, Database db) : base(db) 
        {
            _record = rec;
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
        /// Creates the SQL UPDATE command based on the list of records
        /// and clauses.
        /// </summary>
        public override void CreateCommand()
        {
            base.CreateCommand();

            CreateUpdateSection();
            CreateWhereSection(0, 0); // Fix this!
            _command.CommandText = _commandStr.ToString();
        }

        /*
         * Private class methods
         */
        /// <summary>
        /// Creates the Update section of the Update SQL command.
        /// </summary>
        private void CreateUpdateSection()
        {
            _commandStr.Append(String.Format("UPDATE {1}{0}{2}\nSET ", 
											_table,
											EscapeCharacters[0],
											EscapeCharacters[1]));

            ClauseType              type;
            WhereClause             clause = null;
            string                  clauseStr;
            string                  property = Record.SearchProperty;
            List<OdbcParameter>    paramList = new List<OdbcParameter>();

            /*
             * If there is property specifically provided when
             * search for an object to update create the one clause
             * statement.  Otherwise put every property into the
             * WHERE section of the update clause.
             */
            if (!String.IsNullOrEmpty(property))
            {
                type = ClauseType.Neither;

                clauseStr = String.Format("{0}/=/{1}",
                                            property,
                                            _record.Properties[property]);

                clause = new WhereClause(clauseStr, _table, type);
                SetClauseDataType(ref clause, property);
                _clauses.Add(clause);
            }
            else
            {
                int propertyCount = 0;
                foreach (var key in _record.Properties.Keys)
                {
                    if (propertyCount < _record.Properties.Count - 1)
                        type = ClauseType.And;
                    else
                        type = ClauseType.Neither;

                    clauseStr = String.Format("{0}/=/{1}",
                                                    key,
                                                    _record.Properties[key]);

                    clause = new WhereClause(clauseStr, _table, type);
                    SetClauseDataType(ref clause, key);
                    _clauses.Add(clause);
                    propertyCount++;
                }
            }
            CreateSet();
        }

        /// <summary>
        /// Creates the SET section of the update statement.  This method
        /// gets the list of identity columns for the table and makes sure
        /// not to add them as a part of the list of properties being updated.
        /// This is done to prevent primary keys or identity columns from being
        /// manually updated.
        /// </summary>
        private void CreateSet()
        {
            short i = 0;
            foreach (var key in _record.Properties.Keys)
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
                        if (x.IsPrimaryKey &&
                            x.Column.Equals(key))
                            return true;
                        else
                            return false;
                    });

                if (pkIndex == -1)
                {
                    WhereClause clause = new WhereClause(String.Format("{0}/=/{1}",
                                                                    key,
                                                                    _record.Properties[key]));
                    SetClauseDataType(ref clause, key);

                    OdbcParameter param = BuildParam(ref clause, "U" + Convert.ToString(i));
                    if (i != _record.Properties.Count - 1)
                    {
                        _commandStr.Append(String.Format("{2}{0}{3} = {1}\n\t, ",
                                                       clause.Column,
                                                       param.ParameterName,
													   EscapeCharacters[0],
													   EscapeCharacters[1]));
                    }
                    else
                    {
                        _commandStr.Append(String.Format("{2}{0}{3} = {1}",
                                                        clause.Column,
                                                        param.ParameterName,
														EscapeCharacters[0],
														EscapeCharacters[1]));
                    }
                    _command.Parameters.Add(param);
                }
                i++;
            }
        }

        private void SetClauseDataType(ref WhereClause clause, object key)
        {
            if (_record.Properties[key] != null)
            {
                clause.DataType = _record.Properties[key].GetType();
                /*
                 * I have to check if it is of a byte array type because
                 * if it is I have to make sure the Value property is
                 * properly set.  Otherwise it sets it to the String
                 * "System.Byte[]" instead of the right value.
                 */
                if (_record.Properties[key].GetType() == typeof(byte[]))
                    clause.Value = (byte[])_record.Properties[key];
            }
            else
                clause.DataType = null;
        }
    }
}
