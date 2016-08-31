using DbWrapper.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DbWrapper.SqlServer {

	public class DynamicSqlUpdate : DynamicSqlCommand {

		private Record _record;

		public DynamicSqlUpdate(Record rec, string table, DynamicDatabase db)
			: base(db) {
			_record = rec;
		}

		public override void CreateCommand() {
			base.CreateCommand();
		}

		private void CreateUpdateSection() {
			_commandStr.Append(
				String.Format(
				"UPDATE {1}{0}{2}\nSET ",
				this.Table,
				this.EscapeCharacters[0],
				this.EscapeCharacters[1]
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
			}
		}

	}

}
