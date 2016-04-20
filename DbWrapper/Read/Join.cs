
namespace DbWrapper.Read {
	public enum JoinType : byte {
		Inner,
		Outer,
		Left,
		Right,
		Full
	}

	public struct Join {

		private string _firstColumn;    // The column from the first table being joined on
		private string _secondTable;    // The table that you are joining with
		private string _secondColumn;   // The second table's column to join on
		private JoinType _type;           // The type of join (inner, left, outer, etc.)

		/// <summary>
		/// Gets or sets the name of the table to join the
		/// first table with.
		/// </summary>
		public string Table {
			get { return this._secondTable; }
			set { this._secondTable = value; }
		}

		/// <summary>
		/// Gets or sets the first column of the table 
		/// being joined on.
		/// </summary>
		public string Column {
			get { return this._firstColumn; }
			set { this._firstColumn = value; }
		}
		/// <summary>
		/// Gets or sets the name of the column joining the
		/// join table with the original table.
		/// </summary>
		public string JoinColumn {
			get { return this._secondColumn; }
			set { this._secondColumn = value; }
		}

		/// <summary>
		/// Gets or sets the type of join being performed.
		/// </summary>
		public JoinType Type {
			get { return this._type; }
			set { this._type = value; }
		}
	}
}
