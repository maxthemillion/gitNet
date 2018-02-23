// ---prepare database schema---

// create constraints (and implicitly indices)
CREATE CONSTRAINT ON (o:OWNER) ASSERT o.login IS UNIQUE
// CREATE CONSTRAINT ON (o:OWNER) ASSERT o.gha_id IS UNIQUE

CREATE CONSTRAINT ON (u:USER) ASSERT u.login IS UNIQUE
CREATE CONSTRAINT ON (user:USER) ASSERT user.ght_id IS UNIQUE
CREATE CONSTRAINT ON (user:USER) ASSERT user.gha_id IS UNIQUE

CREATE CONSTRAINT ON (r:RELEASE) ASSERT r.gha_id IS UNIQUE

CREATE CONSTRAINT ON (r:GHA_REPO) ASSERT r.gha_id IS UNIQUE

CREATE CONSTRAINT ON (r:GHT_REPO) ASSERT r.ght_id IS UNIQUE


CREATE CONSTRAINT ON (i:ISSUE) ASSERT i.event_id IS UNIQUE
CREATE CONSTRAINT ON (i:ISSUE) ASSERT i.gha_id IS UNIQUE
CREATE CONSTRAINT ON (i:ISSUE) ASSERT i.url IS UNIQUE

CREATE CONSTRAINT ON (c:COMMIT) ASSERT c.sha IS UNIQUE

CREATE CONSTRAINT ON (i:PULLREQUEST) ASSERT i.event_id IS UNIQUE
CREATE CONSTRAINT ON (r:PULLREQUEST) ASSERT r.gha_id IS UNIQUE

CREATE CONSTRAINT ON (i:COLLABORATOR) ASSERT i.event_id IS UNIQUE

CREATE CONSTRAINT ON (i:COMMENT) ASSERT i.event_id IS UNIQUE
CREATE CONSTRAINT ON (c:COMMENT) ASSERT c.gha_id is UNIQUE;

// --- delete existing data ---
MATCH (n) DETACH DELETE n

// --- import data ---



// -- import distinct users
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/Users_prep" AS row
CREATE(:USER {
  gha_id: toInt(row.actor_id)
});


// -- import distinct owners
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/selected_owners" AS row
CREATE(:OWNER {
  ght_id: toInt(row.ght_owner_id),
  login: row.owner_login
});

// -- import GHA repositories
// -- match gha info on repos with events recorded
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/gha_repos_5" AS row
CREATE(r:GHA_REPO{
  gha_id: toInt(row.gha_id),
  full_name:row.repo_name
});

// -- import GHT repositories
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/selected_repos" AS row
CREATE(:GHT_REPO {
  ght_id:toInt(row.ght_repo_id),
  full_name:row.full_name
});

// -- relate owners and repos
EXPLAIN
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/selected_repos" AS row
MATCH(o:OWNER{login: row.owner_login})
MATCH(r:GHA_REPO{gha_id: toInt(row.gha_repo_id)})
MERGE(r)-[:belongs_to]->(o);

// -- IssuesEvent
// create nodes
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/IssuesEvent_prep" AS row
CREATE (:ISSUE{
	event_id:toInt(row.event_id),
  event_time:row.event_time,
  gha_id:toInt(row.issue_id),
	url:row.issue_url,
  actor_id:toInt(row.actor_id)}
);

EXPLAIN
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/IssuesEvent_prep" AS row
MATCH(i:ISSUE{gha_id: toInt(row.issue_id)})
MATCH(r:GHA_REPO{gha_id: toInt(row.repo_id)})
MERGE (i)-[:to]->(r);



// -- PullRequestEvent
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/PullRequestEvent_prep" AS row
CREATE (:PULLREQUEST{
  event_id:toInt(row.event_id),
  event_time:row.event_time,
  gha_id:toInt(row.pull_request_id),
	issue_url: row.issue_url,
	actor_id:toInt(row.actor_id)
});

EXPLAIN
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/PullRequestEvent_prep" AS row
MATCH(p:PULLREQUEST{gha_id: toInt(row.pull_request_id)})
MATCH(r:GHA_REPO{gha_id: toInt(row.base_repo_id)})
MERGE (p)-[:to]->(r);


// TODO: relationship between pull requests and issues
// this might not connect all issues to pullrequests -> check!

MATCH (i:ISSUE)
MATCH (p:PULLREQUEST)
WHERE i.url = p.issue_url
MERGE (i)-[:is]->(p)




// -- MemberEvent
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/MemberEvent_prep" AS row
CREATE (:COLLABORATOR{
  event_id:toInt(row.event_id),
  event_time:row.event_time,
  actor_id:toInt(row.actor_id),
  member_id:toInt(row.member_id)
  });

// creates less relationships than there are collaborator nodes
// cause: removed repositories which didn't have unique name- / gha_id combination
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/MemberEvent_prep" AS row
MATCH (c:COLLABORATOR{event_id: toInt(row.event_id)})
MATCH (r:GHA_REPO{gha_id: toInt(row.repo_id)})
MERGE (c)-[:to]->(r)



// -- ReleaseEvent
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/ReleaseEvent_prep" AS row
CREATE (:RELEASE{
  event_id:toInt(row.event_id),
  event_time:row.event_time,
  actor_id:toInt(row.actor_id),
  gha_id:toInt(row.release_id)
});


USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/ReleaseEvent_prep" AS row
MATCH (r:GHA_REPO{gha_id:toInt(row.repo_id)})
MATCH (release:RELEASE{gha_id:toInt(row.release_id)})
MERGE (release)-[:to]->(r);



// -- Import PullRequestComments
// TODO: field pull_request_id is empty. Why?
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/PullRequestReviewCommentEvent_prep" AS row
CREATE (:COMMENT{
	event_id:toInt(row.event_id),
	event_time:row.event_time,
	repo_id:toInt(row.repo_id),
	repo_full_name:row.repo_name,
	actor_id:toInt(row.actor_id),
  gha_id:toInt(row.comment_id),
	pull_request_id: toInt(row.pull_request_id)
});

// TODO: connect pull request comments to pull requests





// -- Import IssueComments
EXPLAIN
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/IssueCommentEvent_prep" AS row
CREATE (c:COMMENT{
	event_id:toInt(row.event_id),
	event_time:row.event_time,
  gha_id:toInt(row.comment_id)
})
WITH c, toInt(row.actor_id) as actor
MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
MERGE (u)-[:makes]->(c);

// test code to add relationships when user nodes had already been created
EXPLAIN
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/IssueCommentEvent_prep" AS row
  WITH toInt(row.comment_id) as comment, toInt(row.actor_id) as actor
  MATCH (c:COMMENT{gha_id:comment}) USING INDEX c:COMMENT(gha_id)
  WITH c, actor
  MATCH (u:USER{gha_id: actor}) USING INDEX u:USER(gha_id)
  MERGE (u)-[:makes]->(c);


// creates way less relationships than there are issue comment nodes. Why?
// because comments can also be made to issues which have been created before Jan 2016
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/IssueCommentEvent_prep" AS row
MATCH (i:ISSUE{gha_id:toInt(row.issue_id)})
MATCH (ic:COMMENT{gha_id:toInt(row.comment_id)})
MERGE (ic)-[:to]->(i);

// -- Import CommitComments

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///Export_DataPrep/CommitCommentEvent_prep" AS row
CREATE (:COMMENT{
	event_id:toInt(row.event_id),
	event_time:row.event_time,
  actor_id:toInt(row.actor_id),
  gha_id:toInt(row.comment_id),
	commit_sha: row.commit_id
});

// TODO: connect commit comments to commits

// -- connecting comments to repos
// no matching to repos at this point because connection to repos should be done via issues, pullrequests and commits
// matching to repos could be done directly, if the indirect way fails.


// -- importing commits
// TODO: import commits




// -- import GHT user information
// TODO: import GHT user information




// -- connecting all events to users

// Collaborator
EXPLAIN
MATCH(c:COLLABORATOR)
MATCH(u:USER{gha_id: c.actor_id})
MERGE (u)-[:announces]->(c)

// creates way less relationships than there are collaborator events
EXPLAIN
MATCH(c:COLLABORATOR)
MERGE(u:USER{gha_id: c.member_id})
WITH c, u
MERGE (u)-[:becomes]->(c)

// ISSUE
EXPLAIN
MATCH (i:ISSUE)
MATCH (u:USER{gha_id: i.actor_id})
WITH i, u
MERGE (u)-[:reports]->(i);






