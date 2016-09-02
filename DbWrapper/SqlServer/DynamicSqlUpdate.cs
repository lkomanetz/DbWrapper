using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer {

	public class DynamicSqlUpdate : DynamicSqlCommand {

		private Record _record;

		public DynamicSqlUpdate(Record rec, string table, DynamicDatabase db)
			: base(db) {
			_record = rec;
			this.Table = table;
		}

		public override void InitializeCommand() {
			base.InitializeCommand();
			base.CreateNeededClauses(_record);
			this.AppendUpdateSection();
			this.AppendSetSection();
			this.AppendWhereSection();
			_command.CommandText = _commandStr.ToString();
		}

		private void AppendUpdateSection() {
			_commandStr.AppendFormat(
				"UPDATE {1}{0}{2}\n",
				this.Table,
				this.EscapeCharacters[0],
				this.EscapeCharacters[1]
			);
		}

		private void AppendSetSection() {
			_commandStr.Append("SET ");

			short i = 0;
			foreach (var key in _record.Properties.Keys) {
				int pkIndex = _record.IdentityColumns.FindIndex(x => {
					if (x.IsPrimaryKey &&
						x.Column.Equals(key)) {
						return true;
					}
					else {
						return false;
					}
				});

				if (pkIndex > -1) {
					i++;
					continue;
				}

				WhereClause clause = new WhereClause(key.ToString(), "=", _record.Properties[key], _record.Table);
				OdbcParameter param = clause.BuildParameter();

				if (i != _record.Properties.Count - 1) {
					_commandStr.AppendFormat(
						"{2}{0}{3} = {1}\n\t, ",
						clause.Column,
						param.ParameterName,
						EscapeCharacters[0],
						EscapeCharacters[1]
					);
				}
				else {
					_commandStr.AppendFormat(
						"{2}{0}{3} = {1}\n",
						clause.Column,
						param.ParameterName,
						EscapeCharacters[0],
						EscapeCharacters[1]
					);
				}

				_command.Parameters.Add(param);
				i++;
			}
		}

	}

}
