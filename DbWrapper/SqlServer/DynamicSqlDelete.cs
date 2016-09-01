using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer
{
	public class DynamicSqlDelete : DynamicSqlCommand {

		private Record _record;
		private string _table;

		public DynamicSqlDelete(Record rec, string table, DynamicDatabase db)
			: base(db) {
			_record = rec;
			_table = table;
		}

		public override void InitializeCommand() {
			base.InitializeCommand();
			base.CreateNeededClauses(_record);
			this.AppendDeleteSection();
			this.AppendWhereSection();
			_command.CommandText = _commandStr.ToString();
		}

		private void AppendDeleteSection() {
			_commandStr.Append($"DELETE\nFROM [{_table}]\n");
		}
	}

}
