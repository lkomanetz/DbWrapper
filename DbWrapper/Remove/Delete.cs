using DbWrapper.Read;
using DbWrapper.Contracts;
using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;

namespace DbWrapper.Remove {
	public class Delete : Command {

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="db"></param>
		public Delete(Record record, string table, Database db)
			: base(db) {
			this._table = table;
			this._record = record;
		}

		public override void CreateCommand() {
			base.CreateCommand();

			ClauseType type;
			WhereClause clause;
			string clauseStr;
			string property = Record.SearchProperty;

			if (String.IsNullOrEmpty(property)) {
				short i = 0;
				foreach (var key in _record.Properties.Keys) {
					if (i < _record.Properties.Count - 1)
						type = ClauseType.And;
					else
						type = ClauseType.Neither;

					clauseStr = String.Format(
						"{0}/=/{1}",
						key,
						_record.Properties[key]
					);

					clause = new WhereClause(clauseStr, _table, type);

					if (_record.Properties[key] != null) {
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

					_clauses.Add(clause);
					i++;
				}
			}
			else {
				type = ClauseType.Neither;
				clauseStr = String.Format(
					"{0}/=/{1}",
					 property,
					 _record.Properties[property]
				 );

				clause = new WhereClause(clauseStr, _table, type);

				if (_record.Properties[property] != null) {
					clause.DataType = _record.Properties[property].GetType();

					if (_record.Properties[property].GetType() == typeof(byte[]))
						clause.Value = (byte[])_record.Properties[property];
				}
				else
					clause.DataType = null;

				_clauses.Add(clause);

			}
			_commandStr.Append(String.Format("DELETE FROM [{0}]\n", _table));
			CreateWhereSection(0, 0);
			_command.CommandText = _commandStr.ToString();
		}
	}
}