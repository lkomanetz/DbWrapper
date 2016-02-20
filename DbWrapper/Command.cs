using DbWrapper.Read;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace DbWrapper
{
    public abstract class Command : IDisposable
    {
        /*
         * Private class variables
         */
        protected List<WhereClause>         _clauses;    // Stores list of WHERE clauses
        protected Dictionary<string, Join>  _joins;      // Stores list of JOIN clauses
        protected OdbcCommand               _command;    // Single SQL command built with SQL Parameters
        protected string                    _table;      // Table that determines where the query is executed
        protected StringBuilder             _commandStr; // Generated SQL command
        protected Database                  _db;         // Database object for opening/closing connections
        protected Hashtable                 _columnList; // List of columns for designated table and data types

        /*
         * Class constructors
         */
        /// <summary>
        /// Builds a query based on the table and clause
        /// passed into the constructor.
        /// </summary>
        public Command()
        {
            if (_clauses == null)
                _clauses = new List<WhereClause>();

            if (_joins == null)
                _joins = new Dictionary<string, Join>();

            _table = String.Empty;
            _command = new OdbcCommand();
            _commandStr = new StringBuilder();
            _columnList = new Hashtable();
        }

        /// <summary>
        /// Builds a query based on the database passed 
        /// into the constructor
        /// </summary>
        /// <param name="db"></param>
        public Command(Database db)
        {
            if (_clauses == null)
                _clauses = new List<WhereClause>();

            if (_joins == null)
                _joins = new Dictionary<string, Join>();

            _table = String.Empty;
            _command = new OdbcCommand();
            _db = db;
            _columnList = new Hashtable();
        }

        /*
         * Public class properties
         */
        /// <summary>
        /// Determines what table the query is ran on
        /// </summary>
        public string Table
        {
            get { return _table; }
            set { this._table = value; }
        }

        /// <summary>
        /// Gets and sets the Database object
        /// </summary>
        public Database Database
        {
            get { return this._db; }
            set { this._db = value; }
        }

        /// <summary>
        /// Gets the list of clauses used for WHERE statements
        /// </summary>
        public List<WhereClause> Clauses
        {
            get { return this._clauses; }
        }

        /// <summary>
        /// Gets the list of joins used for the SQL command
        /// </summary>
        public Dictionary<string, Join> Joins
        {
            get { return this._joins; }
        }

		/// <summary>
		/// Determines the escape characters used for database, table,
		/// and column names using the name of the database provider.
		/// </summary>
		protected char[] EscapeCharacters 
		{
			get 
			{
				switch (_db.Engine)
				{
					case DatabaseEngine.MySQL:
						return new char[] { '`', '`' };
					case DatabaseEngine.SqlServer:
						return new char[] { '[', ']' };
					default:
						return new char[] { '`', '`' };
				}
			}
		}

        /*
         * Public virtual methods
         */
        /// <summary>
        /// Creates the SQL Command based on the list of clauses
        /// </summary>
        public virtual void CreateCommand()
        {
            if (_command == null)
                _command = new OdbcCommand();

            if (_commandStr == null)
                _commandStr = new StringBuilder();

            _columnList = Database.GetColumns(_table);
        }

        /*
         * Public override methods
         */
        /// <summary>
        /// Generates the SQL statement
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return _command.CommandText;
        }

        /*
         * Protected class methods
         */
        /// <summary>
        /// Creates a SQL parameter out of the passed in WHERE clause
        /// </summary>
        /// <param name="clause"></param>
        /// <returns>SqlParameter param</returns>
        protected OdbcParameter BuildParam(ref WhereClause clause, 
                                            string suffix)
        {
            OdbcParameter param = new OdbcParameter();
            string paramName = RemoveIllegalCharacters(clause.Column);
            
            //param.ParameterName = String.Format("@{0}{1}", paramName, suffix);
            param.ParameterName = "?";
            param.Value = clause.Value;

            if (clause.DataType != null)
            {
                TypeConverter tc = TypeDescriptor.GetConverter(param.DbType);

                if (tc.CanConvertFrom(clause.DataType))
                    param.DbType = (DbType)tc.ConvertFrom(clause.DataType.Name);
                else
                {
                    try
                    {
                        param.DbType = (DbType)tc.ConvertFrom(clause.DataType.Name);
                    }
                    catch
                    {
                        if (clause.DataType == typeof(byte[]))
                        {
                            param.DbType = DbType.Binary;
                            param.Size = -1;
                        }
                    }
                }
            }
            return param;
        }

        /// <summary>
        /// Removes the @ symbol from the name of the parameter
        /// along with the parameter type (W/I/U) at the end.
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        protected string Trim(ref OdbcParameter param)
        {
            Match match = Regex.Match(param.ParameterName,
                                        @"(^@)(\w+.*)([U|W|I]{1}[0-9]*)$",
                                        RegexOptions.None);
            return match.Groups[2].Value;
        } 

        /// <summary>
        /// Generates the WHERE section of the SQL statement with at least
        /// the amount of rows to return.  If there are clauses that need
        /// to be accounted for, the method adds them in before stating
        /// which subset of records to get.
        /// </summary>
        protected void CreateWhereSection(int rowStart, int rowEnd)
        {
            if (_clauses.Count > 0)
            {
                _commandStr.Append("\nWHERE (");
                for (short i = 0; i < _clauses.Count; i++)
                {
                    WhereClause clause = _clauses[i];
                    OdbcParameter param = BuildParam(ref clause, "W" + Convert.ToString(i));

                    string tableName = String.Empty;

                    /*
                     * If the WhereClause object doesn't have a table
                     * specifically designated for the clause, use the
                     * table within the Record object.  Otherwise use 
                     * the table specifically designated for the clause.
                     */
                    if (clause.Table.Equals(""))
                        tableName = _table;
                    else
                        tableName = clause.Table;

                    // If this WHERE sectiion generation is not for the CTE section
                    if (!(rowStart == 0 && rowEnd == 0))
                    {
						if (_db.Engine == DatabaseEngine.SqlServer)
							tableName = String.Format("{0}Tbl", tableName);
                    }
                    
                    /*
                     * If this WHERE section generation is not for the CTE section
                     * and the table for the where clause is specifically designated
                     * set the name of the table to WhereClause object's table
                     */
                    if (!(rowStart == 0 && rowEnd == 0) &&
                            !clause.Table.Equals(""))
                    {
                        tableName = clause.Table;
                    }

					/*
					 * AND: table.column [operator] [value] AND
					 * OR: table.column [operator] [value] OR
					 * NEITHER: table.column [operator] [value]
					 * 
					 * The table and column values are surrounded by escape characters.
					 */
                    switch (clause.Type)
                    {
                        case ClauseType.And:
                            _commandStr.Append(String.Format("{0}.{4}{1}{5} {2} ? {3}\n\t",
                                        tableName,
                                        clause.Column,
                                        clause.Operator,
                                        "AND",
										EscapeCharacters[0],
										EscapeCharacters[1]));
                            break;
                        case ClauseType.Or:
                            _commandStr.Append(String.Format("{4}{0}{5}.{4}{1}{5} {2} ? {3}\n\t",
                                        tableName,
                                        clause.Column,
                                        clause.Operator,
                                        "OR",
										EscapeCharacters[0],
										EscapeCharacters[1]));
                            break;
                        default:
                            _commandStr.Append(String.Format("{3}{0}{4}.{3}{1}{4} {2} ?",
                                        tableName,
                                        clause.Column,
                                        clause.Operator,
										EscapeCharacters[0],
										EscapeCharacters[1]));
                            break;
                    }

                    //if (_command.Parameters.Contains(param.ParameterName) == false)
                        _command.Parameters.Add(param);
                }
                _commandStr.Append(")");
				if (rowStart != 0 && rowEnd != 0) 
				{
					if (_db.Engine == DatabaseEngine.MySQL) 
					{
						// MySQL LIMIT is 0 based
						_commandStr.AppendFormat(" LIMIT {0},{1}", rowStart - 1, rowEnd);
					}
					else if (_db.Engine == DatabaseEngine.SqlServer) 
					{
						_commandStr.AppendFormat(" AND\n\t(RowNumber >= {0} AND\n\t" +
												"RowNumber <= {1})", rowStart, rowEnd);
					}
				}
            }
            else
            {
				if (rowStart != 0 && rowEnd != 0) 
				{
					if (_db.Engine == DatabaseEngine.MySQL) 
					{
						_commandStr.AppendFormat(" WHERE (RowNumber >= {0} AND\n\t" +
											"RowNumber <= {1})", rowStart, rowEnd);
					}
					else if (_db.Engine == DatabaseEngine.SqlServer) 
					{
						// MySQL LIMIT is 0 based
						_commandStr.AppendFormat(" LIMIT {0},{1}", rowStart - 1, rowEnd);
					}
				}
            }
        }

        /// <summary>
        /// Generates the join section of a command
        /// </summary>
        protected void CreateJoinSection(bool isForCTEGeneration)
        {
			foreach (var key in _joins.Keys)
			{
				switch (_joins[key].Type)
				{
					case JoinType.Inner:
						_commandStr.Append(String.Format("\n\tINNER JOIN {1}{0}{2}",
														_joins[key].Table,
														EscapeCharacters[0],
														EscapeCharacters[1]));
						break;
					case JoinType.Outer:
						_commandStr.Append(String.Format("\n\tOUTER JOIN {1}{0}{2}",
														_joins[key].Table,
														EscapeCharacters[0],
														EscapeCharacters[1]));
						break;
					case JoinType.Left:
						_commandStr.Append(String.Format("\n\tLEFT JOIN {1}{0}{2}",
														_joins[key].Table,
														EscapeCharacters[0],
														EscapeCharacters[1]));
						break;
					case JoinType.Full:
						_commandStr.Append(String.Format("\n\tFULL JOIN {1}{0}{2}",
														_joins[key].Table,
														EscapeCharacters[0],
														EscapeCharacters[1]));
						break;
					case JoinType.Right:
						_commandStr.Append(String.Format("\n\tRIGHT JOIN {1}{0}{2}",
														_joins[key].Table,
														EscapeCharacters[0],
														EscapeCharacters[1]));
						break;
				}

				string table = String.Empty;
				if (!isForCTEGeneration &&
					_db.Engine == DatabaseEngine.SqlServer)
				{
					table = _table.Insert(_table.Length, "Tbl");
				}
				else if (isForCTEGeneration)
					table = _table;
				else
					table = String.Format("{0}{1}{2}",
											EscapeCharacters[0],
											_table,
											EscapeCharacters[1]);

				if (!isForCTEGeneration)
					_commandStr.Append(String.Format("\n\t\tON {0}.{4}{1}{5} = {4}{2}{5}.{4}{3}{5}",
														table,
														_joins[key].Column,
														_joins[key].Table,
														_joins[key].JoinColumn,
														EscapeCharacters[0],
														EscapeCharacters[1]));
				else
					_commandStr.Append(String.Format("\n\t\tON {4}{0}{5}.{4}{1}{5} = {4}{2}{5}.{4}{3}{5}",
														table,
														_joins[key].Column,
														_joins[key].Table,
														_joins[key].JoinColumn,
														EscapeCharacters[0],
														EscapeCharacters[1]));
			}
        }
        
        /// <summary>
        /// Gets the CTE that adds the RowNumber used for dynamic paging.
		/// *NOTE* Only used for MS SQL Server
        /// </summary>
        protected void CreateCTESection()
        {
            // Get the total number of records in the table
            _commandStr.AppendFormat("SELECT COUNT(*) AS RecordCount\n" +
                                    "FROM [{0}];\n\n", _table);

            /*
             * This WITH statement returns the number of rows in the table.
             * It is used when retrieving data to make sure I am retrieving
             * only a subset of data each time.  Primary keys can't be used
             * since once records get deleted it throws everything off.
             */
            _commandStr.AppendFormat("WITH {0}Tbl AS\n(\nSELECT ", _table);

            short i = 0;
            string firstCol = String.Empty;
            foreach (string columnName in _columnList.Keys)
            {
                if (i == 0)
                    firstCol = columnName;

                if (i != _columnList.Count - 1)
                    _commandStr.Append(String.Format("[{0}].[{1}]\n\t, ", _table, columnName));
                else
                    _commandStr.Append(String.Format("[{0}].[{1}]\n", _table, columnName));

                i++;
            }
            _commandStr.AppendFormat("\t, ROW_NUMBER() OVER(ORDER BY [{0}].[{1}]) AS RowNumber\n",
                                        _table,
                                        firstCol);
            _commandStr.AppendFormat("FROM [{0}]", _table);
            CreateJoinSection(true); // The 'true' parameter says it is for CTE generation
            CreateWhereSection(0, 0); // The '0, 0' int parameters says it is for CTE generation
            _commandStr.Append("\n)\n");
        }

        /*
         * Private class methods 
         */
        private string RemoveIllegalCharacters(string paramName)
        {
            return Regex.Replace(paramName,
                                 @"\.",
                                 "");
        }

        /*
         * Public class methods
         */
        /// <summary>
        /// Adds a WHERE clause to the list of clauses
        /// </summary>
        /// <param name="clause"></param>
        public void AddClause(WhereClause clause, ClauseType type = ClauseType.Neither)
        {
            if (!_clauses.Contains(clause))
                _clauses.Add(clause);
        }

        public void AddJoin(Join join, JoinType type = JoinType.Inner)
        {
            if (!_joins.ContainsKey(join.Table))
                _joins.Add(join.Table, join);
        }

        /// <summary>
        /// Disposes the objects used in this class from memory
        /// </summary>
        public void Dispose()
        {
            _commandStr.Length = 0;
            _command.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Executes the SQL command provided by Insert/Update/Query objects
        /// </summary>
        /// <returns>DataSet</returns>
        public DataSet Execute()
        {
            DataSet ds = new DataSet();
            OdbcTransaction trans = null;
            try
            {
                using (OdbcDataAdapter da = new OdbcDataAdapter(_command))
                {
                    _db.Open();
                    _command.Connection = Database.Connection;
                    trans = Database.Connection.BeginTransaction();
                    _command.Transaction = trans;
                    da.Fill(ds);
                    trans.Commit();
                    _commandStr.Length = 0;
                }
            }
            catch (Exception e)
            {
                trans.Rollback();
                throw;
            }
            finally
            {
                trans.Dispose();
                trans = null;
                _db.Close();
            }

            return ds;
        }
    }
}