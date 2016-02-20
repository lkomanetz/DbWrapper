using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DbWrapper
{
    public class TableConstraintInfo
    {
        /*
         * Private class variables
         */
        private string  _column;
        private bool    _isPrimary;

        /*
         * Constructors
         */
        /// <summary>
        /// Default constructor
        /// </summary>
        public TableConstraintInfo()
        {
            Init();
        }

        /*
         * Public class properties
         */
        public string Column 
        {
            get { return _column; }
            set { _column = value; }
        }

        public bool IsPrimaryKey
        {
            get { return _isPrimary; }
            set { _isPrimary = value; }
        }

        /*
         * Private class methods
         */
        /// <summary>
        /// Initializes the TableConstraintInfo class
        /// </summary>
        private void Init()
        {
            _column = String.Empty;
            _isPrimary = false;
        }
    }
}
