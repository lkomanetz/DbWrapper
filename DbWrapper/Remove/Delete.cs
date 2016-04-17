using DbWrapper.Read;
using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;

namespace DbWrapper.Remove {
	public class Delete : Command {
		/*
		 * Private class variables
		 */
		private Record _record;

		/*
		 * Public class constructors
		 */
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
				foreach (var key in _record.PropertyList.Keys) {
					if (i < _record.PropertyList.Count - 1)
						type = ClauseType.And;
					else
						type = ClauseType.Neither;

					clauseStr = String.Format(
						"{0}/=/{1}",
						key,
						_record.PropertyList[key]
					);

					clause = new WhereClause(clauseStr, _table, type);

					if (_record.PropertyList[key] != null) {
						clause.DataType = _record.PropertyList[key].GetType();

						/*
						 * I have to check if it is of a byte array type because
						 * if it is I have to make sure the Value property is
						 * properly set.  Otherwise it sets it to the String
						 * "System.Byte[]" instead of the right value.
						 */
						if (_record.PropertyList[key].GetType() == typeof(byte[]))
							clause.Value = (byte[])_record.PropertyList[key];
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
					 _record.PropertyList[property]
				 );

				clause = new WhereClause(clauseStr, _table, type);

				if (_record.PropertyList[property] != null) {
					clause.DataType = _record.PropertyList[property].GetType();

					if (_record.PropertyList[property].GetType() == typeof(byte[]))
						clause.Value = (byte[])_record.PropertyList[property];
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