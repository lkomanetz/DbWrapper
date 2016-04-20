using DbWrapper.Read;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Text;

namespace DbWrapper {

	public class Update : Command {

		public Update(Record rec, string table, Database db) : base(db) {
			_record = rec;
			_table = table;
		}

		public override string ToString() {
			CreateCommand();
			return base.ToString();
		}

		/// <summary>
		/// Creates the SQL UPDATE command based on the list of records
		/// and clauses.
		/// </summary>
		public override void CreateCommand() {
			base.CreateCommand();

			CreateUpdateSection();
			base.CreateWhereSection(0, 0); // Fix this!
			_command.CommandText = _commandStr.ToString();
		}

		/// <summary>
		/// Creates the Update section of the Update SQL command.
		/// </summary>
		private void CreateUpdateSection() {
			_commandStr.Append(
				String.Format(
					"UPDATE {1}{0}{2}\nSET ",
					_table,
					EscapeCharacters[0],
					EscapeCharacters[1]
				)
			);

			/*
			 * I only need the one clause if the SearchProperty was provided.  Otherwise
			 * I use all properties and their values for searching.
			 */
			if (!String.IsNullOrEmpty(Record.SearchProperty)) {
				string clauseStr = String.Format(
					"{0}/=/{1}",
					Record.SearchProperty,
					_record.Properties[Record.SearchProperty]
				);

				WhereClause clause = new WhereClause(clauseStr, _table, ClauseType.Neither);
				SetClauseDataType(ref clause, Record.SearchProperty);
				_clauses.Add(clause);
			}
			else {
				int propertyCount = 0;
				foreach (var key in _record.Properties.Keys) {
					ClauseType type = ClauseType.Neither;
					if (propertyCount < _record.Properties.Count - 1) {
						type = ClauseType.And;
					}

					string clauseStr = String.Format(
						"{0}/=/{1}",
						key,
						_record.Properties[key]
					);

					WhereClause clause = new WhereClause(clauseStr, _table, type);
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
		private void CreateSet() {
			short i = 0;
			foreach (var key in _record.Properties.Keys) {
				int fkIndex = _record.IdentityColumns.FindIndex(x => {
					if (x.IsPrimaryKey == false &&
						x.Column.Equals(key))
						return true;
					else
						return false;
				});
				int pkIndex = _record.IdentityColumns.FindIndex(x => {
					if (x.IsPrimaryKey &&
						x.Column.Equals(key))
						return true;
					else
						return false;
				});

				if (pkIndex == -1) {
					string clauseStr = String.Format(
						"{0}/=/{1}",
						key,
						_record.Properties[key]
					);
					WhereClause clause = new WhereClause(clauseStr);
					SetClauseDataType(ref clause, key);

					OdbcParameter param = BuildParam(ref clause, "U" + Convert.ToString(i));
					if (i != _record.Properties.Count - 1) {
						_commandStr.Append(
							String.Format(
								"{2}{0}{3} = {1}\n\t, ",
								clause.Column,
								param.ParameterName,
								EscapeCharacters[0],
								EscapeCharacters[1]
							)
						);
					}
					else {
						_commandStr.Append(
							String.Format(
								"{2}{0}{3} = {1}",
								clause.Column,
								param.ParameterName,
								EscapeCharacters[0],
								EscapeCharacters[1]
							)
						);
					}
					_command.Parameters.Add(param);
				}
				i++;
			}
		}

	}

}