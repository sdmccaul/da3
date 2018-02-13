var stats = {};
// stats['db_size'] = show dbs;
stats['db_overview'] =db.stats();
stats['nodes'] = db.pvd.count({'datatype' : 'node'});
stats['ways'] = db.pvd.count({'datatype' : 'way'});
stats['relations'] = db.pvd.count({'datatype' : 'relation'});
stats['distinct_users'] = db.pvd.distinct('created.user').length;
stats['user_counts'] = db.pvd.aggregate([ {'$group' : { '_id': '$created.user', 'count' : { '$sum' : 1} } }, {$sort : {count : -1 } } ])._batch;
printjson(stats);