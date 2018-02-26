// ---prepare database schema---

// create constraints (and implicitly indices)
CREATE CONSTRAINT ON (o:OWNER) ASSERT o.login IS UNIQUE;
// CREATE CONSTRAINT ON (o:OWNER) ASSERT o.gha_id IS UNIQUE

CREATE CONSTRAINT ON (u:USER) ASSERT u.login IS UNIQUE;
CREATE CONSTRAINT ON (user:USER) ASSERT user.ght_id IS UNIQUE;
CREATE CONSTRAINT ON (user:USER) ASSERT user.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (r:RELEASE) ASSERT r.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (r:GHA_REPO) ASSERT r.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (r:GHT_REPO) ASSERT r.ght_id IS UNIQUE;


CREATE CONSTRAINT ON (i:ISSUE) ASSERT i.event_id IS UNIQUE;
CREATE CONSTRAINT ON (i:ISSUE) ASSERT i.gha_id IS UNIQUE;
CREATE INDEX ON :ISSUE(url);

CREATE CONSTRAINT ON (c:COMMIT) ASSERT c.sha IS UNIQUE;

CREATE CONSTRAINT ON (i:PULLREQUEST) ASSERT i.event_id IS UNIQUE;
CREATE CONSTRAINT ON (r:PULLREQUEST) ASSERT r.gha_id IS UNIQUE;

CREATE CONSTRAINT ON (i:COLLABORATOR) ASSERT i.event_id IS UNIQUE;

CREATE CONSTRAINT ON (i:COMMENT) ASSERT i.event_id IS UNIQUE;
CREATE CONSTRAINT ON (c:COMMENT) ASSERT c.gha_id IS UNIQUE;

// --- delete existing data ---
MATCH (n) DETACH DELETE n;

// --- import data ---



// -- import distinct users
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/Users_prep' AS row
CREATE(:USER {
  gha_id: toInt(row.actor_id)
});

// -- additionally import members (miss in users csv)
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/MemberEvent_prep' AS row
MERGE (:USER {
  gha_id: toInt(row.member_id)
})


// -- import distinct owners
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/selected_owners' AS row
CREATE(:OWNER {
  ght_id: toInt(row.ght_owner_id),
  login: row.owner_login
});

// -- import GHA repositories
// -- match gha info on repos with events recorded
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/gha_repos_5' AS row
CREATE(r:GHA_REPO{
  gha_id: toInt(row.gha_id),
  full_name:row.repo_name
});

// -- import GHT repositories
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/selected_repos' AS row
CREATE(:GHT_REPO {
  ght_id:toInt(row.ght_repo_id),
  full_name:row.full_name
});

// -- relate owners and GHA_repos
// TODO: Matching fails. there is no gha_repo_id in "selected_repos". Fix that.
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/selected_repos' AS row
MATCH(o:OWNER{login: row.owner_login})
MATCH(r:GHA_REPO{gha_id: toInt(row.gha_repo_id)})
MERGE(r)-[:belongs_to]->(o);


// -- IssuesEvent
// create nodes, relate them to users and repos
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/IssuesEvent_prep' AS row
WITH row, apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE (i:ISSUE:EVENT{
gha_id:toInt(row.issue_id),
url:row.issue_url,
event_time: dt
})
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
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/PullRequestEvent_prep' AS row
WITH row, apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE (p:PULLREQUEST:EVENT{
  event_time:dt,
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

// code to connect issues and pr after pr have already been created
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/PullRequestEvent_prep' AS row
WITH toInt(row.pull_request_id) as pr_id, row.issue_url as issue_url
MATCH(i:ISSUE{url: issue_url})
MATCH(p:PULLREQUEST{gha_id: pr_id})
MERGE (i)-[:is]->(p);


// -- MemberEvent
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/MemberEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE (c:COLLABORATOR:EVENT{
  event_id:toInt(row.event_id),
  event_time:dt
  })
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
EXPLAIN
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/ReleaseEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE (r:RELEASE:EVENT{
  event_time:dt,
  gha_id:toInt(row.release_id)
})

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
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/PullRequestReviewCommentEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE (c:PR_COMMENT:COMMENT:EVENT{
	event_time:dt,
  gha_id:toInt(row.comment_id)
})

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
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/IssueCommentEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt
CREATE (c:I_COMMENT:COMMENT:EVENT{
	event_time:dt,
  gha_id:toInt(row.comment_id)
})

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
LOAD CSV WITH HEADERS FROM 'file:///Export_DataPrep/CommitCommentEvent_prep' AS row
WITH row,
     apoc.date.parse(row.event_time, 'ms', 'yyyy-MM-dd hh:mm:ss') as dt,
     row.commit_id as commit_sha
CREATE (c:C_COMMENT:COMMENT:EVENT{
	event_time:dt,
  gha_id:toInt(row.comment_id)
})
WITH c,
     toInt(row.actor_id) AS actor,
     commit_sha
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:makes]->(c)
WITH c,
     commit_sha
MERGE(commit:COMMIT{
  sha: commit_sha
})
WITH c,
     commit
MERGE (c)-[:to]->(commit);


// -- importing commits
// TODO: import commits






// -- import GHT user information
// TODO: import GHT user information





// Queries to verify the database:

MATCH (n:EVENT)-[x]->(m)
with labels(n) as l_n,
     COUNT(DISTINCT n) as ct_n,
     type(x) as t_x,
     COUNT (DISTINCT x) as ct_x,
     labels(m) as l_m,
     COUNT(DISTINCT m) as ct_m
RETURN l_n, ct_n, t_x, ct_x, l_m, ct_m
ORDER BY l_m;

MATCH (n:USER)-[x]->(m)
with labels(n) as l_n,
     COUNT(DISTINCT n) as ct_n,
     type(x) as t_x,
     COUNT (DISTINCT x) as ct_x,
     labels(m) as l_m,
     COUNT(DISTINCT m) as ct_m
RETURN l_n, ct_n, t_x, ct_x, l_m, ct_m
ORDER BY l_m;
