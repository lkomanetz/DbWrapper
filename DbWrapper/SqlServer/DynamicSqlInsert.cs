using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer
{
	public class DynamicSqlInsert : DynamicSqlCommand {

		private Record _record;

		public DynamicSqlInsert(Record rec, string table, DynamicDatabase db) 
			: base(db) {

			_record = rec;
			this.Table = table;
		}

		public override void InitializeCommand()
		{
			base.InitializeCommand();
			AppendInsertSection();
			AppendValuesSection();
			_command.CommandText = _commandStr.ToString();
		}

		private void AppendInsertSection() {
			_commandStr.Append($"INSERT INTO [{this.Table}] (\n\t");

			short counter = 0;
			foreach (var propertyName in _record.Properties.Keys) {
				var identityInfo = _record.GetIdentityInfo(propertyName.ToString());
				if (identityInfo != null && identityInfo.IsPrimaryKey) {
					counter++;
					continue;
				}

				if (counter != _record.Properties.Count - 1) {
					_commandStr.Append($"{propertyName},\n\t");
				}
				else {
					_commandStr.Append($"{propertyName})\n");
				}
				counter++;
			}
		}

		private void AppendValuesSection() {
			_commandStr.Append("( ");

			short propertyCount = 0;
			foreach (var propertyName in _record.Properties.Keys) {
				var identityInfo = _record.GetIdentityInfo(propertyName.ToString());
				if (identityInfo != null && identityInfo.IsPrimaryKey) {
					propertyCount++;
					continue;
				}

				WhereClause clause = new WhereClause(
					propertyName.ToString(),
					"=",
					_record.Properties[propertyName],
					_record.Table
				);
				OdbcParameter param = clause.BuildParameter();

				if (propertyCount != _record.Properties.Count - 1) {
					_commandStr.Append($"{param.ParameterName}\n\t, ");
				}
				else {
					_commandStr.Append($"{param.ParameterName})");
				}

				_command.Parameters.Add(param);
				propertyCount++;
			}
		}

	}

}
