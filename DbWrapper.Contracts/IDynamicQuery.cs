using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DbWrapper.Contracts {

	public interface IDynamicQuery {
		int CurrentPage { get; set; }
		int RowStart { get; }
		int RowEnd { get; }
	}

}
