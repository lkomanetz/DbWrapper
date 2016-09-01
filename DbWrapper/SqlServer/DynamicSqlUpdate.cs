using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer {

	public class DynamicSqlUpdate : DynamicSqlCommand {

		private Record _record;
		private string _table;
		private WhereClause[] _whereClauses;

		public DynamicSqlUpdate(Record rec, string table, DynamicDatabase db)
			: base(db) {
			_record = rec;
			_table = table;
		}

		public override void InitializeCommand() {
			base.InitializeCommand();
			_whereClauses = CreateNeededClauses();
			this.AppendUpdateSection();
			this.AppendSetSection();
			this.AppendWhereSection();
		}

		protected override void AppendWhereSection() {
			_commandStr.Append("\nWHERE (");
			for (short i = 0; i < _whereClauses.Length; i++) {
				string clauseTypeStr = String.Empty;

				if (_whereClauses[i].Type == ClauseType.And ||
					_whereClauses[i].Type == ClauseType.Or) {

					clauseTypeStr = (_whereClauses[i].Type == ClauseType.And) ? "AND" : "OR";
				}

				if (String.IsNullOrEmpty(clauseTypeStr)) {
					_commandStr.AppendFormat(
						"{3}{0}{4}.{3}{1}{4} {2} ?",
						_table,
						_whereClauses[i].Column,
						_whereClauses[i].Operator,
						EscapeCharacters[0],
						EscapeCharacters[1]
					);
				}
				else {
					_commandStr.AppendFormat(
						"{0}.{4}{1}{5} {2} ? {3}\n\t",
						_table,
						_whereClauses[i].Column,
						_whereClauses[i].Operator,
						clauseTypeStr,
						EscapeCharacters[0],
						EscapeCharacters[1]
					);
				}
				_command.Parameters.Add(_whereClauses[i].BuildParameter());
			}
			_commandStr.Append(")");
		}

		private WhereClause[] CreateNeededClauses() {
			if (!String.IsNullOrEmpty(Record.SearchProperty)) {
				WhereClause clause = new WhereClause(
					$"{Record.SearchProperty}/=/{_record.Properties[Record.SearchProperty]}",
					_table,
					_record.Properties[Record.SearchProperty].GetType(),
					ClauseType.Neither
				);

				OdbcParameter param = BuildParameter(ref clause, $"W0");
				_commandStr.AppendFormat(
					"\nWHERE ({3}{0}{4}.{3}{1}{4} {2} ?)",
					_table,
					clause.Column,
					clause.Operator,
					EscapeCharacters[0],
					EscapeCharacters[1]
				);

				return new WhereClause[] { clause };
			}

			int propertyCount = 0;
			WhereClause[] clauses = new WhereClause[_record.Properties.Count];
			foreach (var key in _record.Properties.Keys) {
				ClauseType type = ClauseType.Neither;
				if (propertyCount < _record.Properties.Count - 1) {
					type = ClauseType.And;
				}

				string clauseStr = $"{key}/=/{_record.Properties[key]}";
				WhereClause clause = new WhereClause(clauseStr,
					_table,
					_record.Properties[key].GetType(),
					type
				);
				clauses[propertyCount] = clause;
				propertyCount++;
			}

			return clauses;
		}

		private void AppendUpdateSection() {
			_commandStr.AppendFormat(
				"UPDATE {1}{0}{2}\n",
				_table,
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

				string clauseStr = $"{key}/=/{_record.Properties[key]}";
				WhereClause clause = new WhereClause(clauseStr, "", _record.Properties[key].GetType());
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
						"{2}{0}{3} = {1}",
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
