package log

import (
	"github.com/mandelsoft/logging"
)

var Realm = logging.DefineRealm("mandelsoft/streaming", "mandelsoft data element steaming")
var Log = logging.DynamicLogger(logging.DefaultContext(), Realm)
