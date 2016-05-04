using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbWrapper.Contracts {

	public interface IDynamicCommand : IDisposable {

		string Table { get; set; }
		DynamicDatabase Database { get; set; }
		List<WhereClause> Clauses { get; set; }
		Dictionary<string, Join> Joins { get; set; }
		char[] EscapeCharacters { get; }

		void CreateCommand();
		DataSet Execute();
		void AddClause(WhereClause clause, ClauseType type = ClauseType.Neither);
		void AddJoin(Join join, JoinType type = JoinType.Inner);

	}

}
