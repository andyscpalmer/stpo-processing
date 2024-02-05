# STPO Processing

This is an experimental tool that utilizes the [Python AT Protocol SDK](https://atproto.blue/en/latest/) along with the [Python Natural Language Toolkit](https://www.nltk.org/) to evaluate posts on the social media app [Bluesky](https://bsky.app/) for their uniqueness compared to posts in the relatively recent past. "STPO" stands for "Separation to Pair Occurrences".

## How it works

Posts from the Bluesky "Firehose" are analyzed in real time and the contents of their text is recorded into a database that enumerates how often two words appear in succession, separated by a certain number of words between them.

For example, given the following posts:
- "post a bad selfie from your phone"
- "post one bad selfie from your phone"
- "without taking a new one, post 1 bad selfie from your phone"

The words "post" and "bad" separated by a single word appears 3 times, which will be recorded as `{1: {"post": {"bad": 3}}}` and similarly the words "post" and "selfie" being separated by two words three times will be recorded as `{2: {"post": {"selfie": 3}}}`, both of which are then recorded into a single dictionary object along with all other pair occurrences.

Common pair occurrences are then archived for use in a feed where new posts are scored based on how similar their wording/phrasing is to older posts.

## Current Status

This project is currently on hold due to efficiency issues with the implementation but could be revisited at a future date.