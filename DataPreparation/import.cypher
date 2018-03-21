// ---prepare database schema---

// create constraints (and implicitly indices)
CREATE CONSTRAINT ON (o:OWNER) ASSERT o.login IS UNIQUE;

CREATE CONSTRAINT ON (u:USER) ASSERT u.login IS UNIQUE;
CREATE CONSTRAINT ON (user:USER) ASSERT user.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (ght_u:GHT_USER) ASSERT ght_u.login IS UNIQUE;
CREATE CONSTRAINT ON (ght_u:GHT_USER) ASSERT ght_u.ght_id IS UNIQUE;

CREATE CONSTRAINT ON (r:RELEASE) ASSERT r.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (r:GHA_REPO) ASSERT r.gha_id IS UNIQUE;
CREATE INDEX ON :GHA_REPO(full_name);

CREATE CONSTRAINT ON (r:GHT_REPO) ASSERT r.ght_id IS UNIQUE;
CREATE CONSTRAINT ON (r:GHT_REPO) ASSERT r.full_name IS UNIQUE;

CREATE CONSTRAINT ON (i:ISSUE) ASSERT i.gha_id IS UNIQUE;
CREATE INDEX ON :ISSUE(url);

CREATE CONSTRAINT ON (c:COMMIT) ASSERT c.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (r:PULLREQUEST) ASSERT r.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (i:COLLABORATOR) ASSERT i.event_id IS UNIQUE;

CREATE CONSTRAINT ON (i:COMMENT) ASSERT i.event_id IS UNIQUE;
CREATE CONSTRAINT ON (c:COMMENT) ASSERT c.gha_id IS UNIQUE;


// --- import data ---


// -- import GHA users
// table gha_actor_ids contains distinct gha_ids of actors and additionally of members who became
// collaborators to a certain project

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_actor_ids' AS row
CREATE(:USER {
  gha_id: toInt(row.actor_id)
});


// -- import GHT owners
// table ght_owners_sample contains ght_ids and logins of owners in the sample
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/ght_owners_sample' AS row
CREATE(:OWNER {
  ght_id: toInt(row.ght_owner_id),
  login: row.owner_login
});

// -- import GHT repositories
// table ght_repos_sample contains ght_id and full repository names of repositories in the sample
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/ght_repos_sample' AS row
CREATE(:GHT_REPO {
  ght_id:toInt(row.ght_repo_id),
  full_name:row.full_name
});

// -- import GHA repositories
// table gha_repos contains all distinct repository ids for which events have been recorded
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_repos_8' AS row
MERGE(r:GHA_REPO{
  gha_id: toInt(row.gha_id)
});


// relate GHA Repos
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_repos_matched' AS row
WITH
	toInt(row.one) as gha_id_1,
	toInt(row.two) as gha_id_2
MATCH (r1:GHA_REPO{gha_id: gha_id_1})
MATCH (r2:GHA_REPO{gha_id:gha_id_2})
MERGE (r1)-[:is]-(r2);


// -- relate owners and GHA_repos
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_repos_8' AS row
MATCH(o:OWNER{login: row.owner_name})
MATCH(r:GHA_REPO{gha_id: toInt(row.gha_id)})
MERGE(r)-[:belongs_to]->(o);


// -- relate owners and GHT_repos
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/ght_repos_sample' AS row
MATCH(o:OWNER{login: row.owner_login})
MATCH(r:GHT_REPO{ght_id: toInt(row.ght_repo_id)})
MERGE(r)-[:belongs_to]->(o);


// -- IssuesEvent
// create nodes, relate them to users and repos
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/IssuesEvent_prep' AS row
WITH row, apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (i:ISSUE{
gha_id:toInt(row.issue_id)
})
ON CREATE SET
i :EVENT,
i.url = row.issue_url,
i.event_time = dt


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/IssuesEvent_prep' AS row
MATCH (i:ISSUE{gha_id:toInt(row.issue_id)})

WITH i,
		 toInt(row.actor_id) AS actor,
		 toInt(row.repo_id) as repo
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:opens]->(i)

WITH i,
     repo
MATCH(r:GHA_REPO{gha_id: repo})
MERGE (i)-[:to]->(r);

// -- PullRequestEvent
// create nodes, relate them to users and repos
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/PullRequestEvent_prep' AS row
WITH row, apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (p:PULLREQUEST{
	gha_id:toInt(row.pull_request_id)
})
ON CREATE SET
p: EVENT,
p.event_time = dt



USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/PullRequestEvent_prep' AS row
	MATCH (p:PULLREQUEST{
		gha_id:toInt(row.pull_request_id)
	})
	WITH p,
     toInt(row.actor_id) AS actor,
     toInt(row.base_repo_id) AS repo,
     row.issue_url as issue
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:requests]->(p)

WITH p,
     repo,
     issue
MATCH(r:GHA_REPO{gha_id: repo})
MERGE (p)-[:to]->(r)

WITH p,
     issue
MATCH (i:ISSUE{url: issue})
MERGE (i)-[:is]->(p);


// TODO: Issues and Pullrequests can only be connected based on issue_url since all other data
// is not available in GHT BQ. MatchQuotes are low. Is there any other way to bring them together more
// reliably?

// -- MemberEvent
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/MemberEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (c:COLLABORATOR{
  event_id:toInt(row.event_id)
})
ON CREATE SET
c: EVENT,
c.event_time = dt

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/MemberEvent_prep' AS row
MATCH (c:COLLABORATOR{event_id: toInt(row.event_id)})

WITH c,
     toInt(row.actor_id) AS actor,
     toInt(row.member_id) as member,
     toInt(row.repo_id) as repo
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:promotes]->(c)

WITH c,
     member,
     repo
MATCH (u:USER{gha_id: member}) USING INDEX u:USER(gha_id)
MERGE (u)-[:becomes]->(c)

WITH c,
     repo
MATCH (r:GHA_REPO{gha_id: repo})
MERGE (c)-[:to]->(r);



// -- ReleaseEvent
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/ReleaseEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (r:RELEASE{gha_id:toInt(row.release_id)})
ON CREATE SET
r : EVENT,
r.event_time = dt


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/ReleaseEvent_prep' AS row
MATCH (r:RELEASE{gha_id:toInt(row.release_id)})
WITH r,
     toInt(row.actor_id) AS actor,
     toInt(row.repo_id) as repo
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:releases]->(r)

WITH r,
     repo
MATCH (rep:GHA_REPO{gha_id:repo})
MERGE (r)-[:to]->(rep);


// -- Import PullRequestComments, connect to actors and pullrequests
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/PullRequestReviewCommentEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (c:COMMENT{gha_id:toInt(row.comment_id)})
ON CREATE SET
c : EVENT,
c : PR_COMMENT,
c.event_time = dt

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/PullRequestReviewCommentEvent_prep' AS row
MATCH (c:COMMENT{gha_id:toInt(row.comment_id)})
	WITH c,
     toInt(row.actor_id) AS actor,
     toInt(row.pull_request_id) as pr_id
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:makes]->(c)

WITH c,
     pr_id
MATCH (pr:PULLREQUEST{gha_id:pr_id})
MERGE (c)-[:to]->(pr);


// -- Import IssueComments, connect to actors and issues
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/IssueCommentEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (c:COMMENT{
  gha_id:toInt(row.comment_id)
})
ON CREATE SET
c: EVENT,
c: I_COMMENT,
c.event_time = dt;

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/IssueCommentEvent_prep' AS row
MATCH (c:COMMENT{gha_id:toInt(row.comment_id)})
WITH c,
     toInt(row.actor_id) AS actor,
     toInt(row.issue_id) as i_id
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:makes]->(c)

WITH c,
     i_id
MATCH (i:ISSUE{gha_id:i_id})
MERGE (c)-[:to]->(i);


// -- Import CommitComments
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/CommitCommentEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
MERGE (c:COMMENT{
	gha_id:toInt(row.comment_id)
})
ON CREATE SET
c : EVENT,
c : C_COMMENT,
c.event_time = dt;

// match commenter and commit comments
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/CommitCommentEvent_prep' AS row
MATCH (c:COMMENT{gha_id:toInt(row.comment_id)})
WITH c,
     toInt(row.actor_id) AS actor,
     row.commit_id as commit_sha
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:makes]->(c);

// -- importing commits
// no commits are allowed to be in the database at time of import

USING PERIODIC COMMIT 1000
LOAD CSV FROM "file:///Export_DataPrep/ght_commits_8" AS row
WITH row[1] as commit_sha,
     toInt(row[2]) as ght_author_id,
     toInt(row[4]) as ght_repo_id,
     apoc.date.parse(row[5], 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE(c:COMMIT:EVENT{
  event_time: dt,
  gha_id: commit_sha
})
WITH c,
     ght_author_id,
     ght_repo_id
MERGE(u:GHT_USER{ght_id:ght_author_id})
CREATE (u)-[:authored]->(c)

WITH c,
     ght_repo_id
MATCH(r:GHT_REPO)
WHERE r.ght_id = ght_repo_id
CREATE(c)-[:to]->(r);


// -- create commits having comments on them and relate them to GHT_REPOS if both commit and relation does not already exist
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/CommitCommentEvent_prep' AS row
WITH toInt(row.ght_repo_id) as ght_repo_id,
	row.commit_id as commit_sha
	MERGE (c:COMMIT{
	gha_id: commit_sha
})

WITH c, ght_repo_id
MATCH (r:GHT_REPO{ght_id: ght_repo_id})
MERGE (c)-[:to]->(r);


// -- import references:
USING PERIODIC COMMIT 1000
LOAD CSV FROM 'file:///Relations/180307_relations.csv' AS row
WITH
	toInt(row[0]) as user_id,
	toInt(row[1]) as comment_id,
	row[2] as ref_type
MATCH (user:USER{gha_id:user_id})
MATCH (comment:COMMENT{gha_id:comment_id})
WITH comment, user, ref_type
CALL apoc.create.relationship(comment, ref_type, {}, user)
YIELD rel
RETURN rel;




// Queries to verify the database:
// view connections

MATCH (n:EVENT)-[x]->(m)
with labels(n) as l_n,
     COUNT(DISTINCT n) as ct_n,
     type(x) as t_x,
     COUNT (DISTINCT x) as ct_x,
     labels(m) as l_m,
     COUNT(DISTINCT m) as ct_m
RETURN l_n, ct_n, t_x, ct_x, l_m, ct_m
ORDER BY l_m;

MATCH (n)-[x]->(m)
with labels(n) as l_n,
     COUNT(DISTINCT n) as ct_n,
     type(x) as t_x,
     COUNT (DISTINCT x) as ct_x,
     labels(m) as l_m,
     COUNT(DISTINCT m) as ct_m
RETURN l_n, ct_n, t_x, ct_x, l_m, ct_m
ORDER BY l_m;






//-- match gha and ght users with the same name

USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_users_to_ght_users_8' AS row
MATCH(ght_u:GHT_USER{ght_id: toInt(row.ght_id)})
MATCH(gha_u:USER{gha_id: toInt(row.gha_id)})
CALL apoc.refactor.mergeNodes([gha_u, ght_u])
YIELD node
RETURN  Count(node);


// -- match gha and ght repos with the same name

USING PERIODIC COMMIT 100
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_ght_repos_matching_8' AS row
WITH
	toInt(row.ght_repo_id) as ght_id,
	toInt(row.gha_id) as gha_id
MATCH (ght_r:GHT_REPO{ght_id: ght_id})
MATCH (gha_r:GHA_REPO{gha_id: gha_id})
CALL apoc.refactor.mergeNodes([gha_r, ght_r])
YIELD node
RETURN  Count(node);

// delete duplicate :belongs_to relationships
MATCH (x:GHT_REPO)-[y:belongs_to]->(o:OWNER)
WITH x, o, type(y) as t, tail(collect(y)) as coll
FOREACH(i in coll | delete i);


// delete repositories which have been attributed to more than one owner.
MATCH (x:GHT_REPO)-[y:belongs_to]->(o:OWNER)
WITH x, COLLECT(o) as ct WHERE SIZE(ct) >1
DETACH DELETE x;

//
MATCH (x:GHT_REPO) SET x:REPO;
CREATE CONSTRAINT ON (r:REPO) ASSERT r.ght_id IS UNIQUE;



// -- create connection between first commenter and creator of issue
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/allEvents/IssuesEvent_prep' AS row
MATCH (i:ISSUE{gha_id:toInt(row.issue_id)})
MATCH (c:COMMENT)-[:to]->(i)
WITH c, i ORDER BY i, c.gha_id

