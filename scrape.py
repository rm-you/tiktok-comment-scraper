import asyncio
import configparser
import datetime
import io

from TikTokApi import TikTokApi

CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")
TOKEN = CONFIG.get("DEFAULT", "ms_token")
MAX_COMMENTS = CONFIG.getint("DEFAULT", "max_comments", fallback=10000)

VIDEOS = [
    '7389708877756943646',
    '7314510966987689246',
    '7272784797930474795',
    '7249851051925409029',
    '7368150159558987014',

    '7347077082490408234',
    '7348286326124416302',
    '7389055002838355230',
    '7426139154137861419',
    '7326797908916096302',

    '7424675691339189534',
    '7427522821766630699',
    '7390480868231449899',
    '7174076486419221802',
    '7359018405728357675',

    '7371242540894326059',
    '7435454553841028394',
    '7278812355948006702',
    '7225792427959831854',
    '7312607220913474846',

    '7338547662628605214',
    '7156800795533446442',
    '7403082373685906731',
    '7209377450084535595',
    '7384471337714060587',

    '7283199050143370538',
]


async def get_replies(comment):
    # There is a bug in the library as of version 6.5.2
    # See: https://github.com/davidteather/TikTok-Api/issues/1181
    comment.author.user_id = comment.as_dict["aweme_id"]

    replies = {}
    async for reply in comment.replies(count=MAX_COMMENTS):
        if reply.id in replies:
            continue
        replies[reply.id] = reply
    # print(f"Returning replies: {len(replies)}")
    return replies


async def get_comments(video, comments):
    async for comment in video.comments(count=MAX_COMMENTS):
        if comment.id in comments:
            continue
        comment_struct = {
            "comment": comment,
            "replies": []
        }
        # print(f"Comment: {comment.text}")
        replies = await get_replies(comment)
        comment_struct["replies"] = replies
        comments[comment.id] = comment_struct
        # Print the count every 100 comments
        if len(comments) % 50 == 0:
            print(f"Comments: {len(comments)}")
    return comments


async def get_all_comments(video_id: str):
    async with TikTokApi() as api:
        # await api.create_sessions(num_sessions=1, sleep_after=5, browser="firefox")
        await api.create_sessions(ms_tokens=[TOKEN], num_sessions=1, sleep_after=3, browser="firefox")
        video = api.video(id=video_id)
        comments = {}
        await get_comments(video, comments)

        return comments


def store_comments_for_video(video_id: str):
    # Get the entire list of comments from the video
    all_comments: dict[int, dict] = asyncio.run(get_all_comments(video_id))

    comment_list = []
    # Iterate through them and store them in CSV format
    for cid, comment in all_comments.items():
        comment_list.append(",".join(
            [
                # CommentID
                comment["comment"].id,
                # UserName
                comment["comment"].author.username,
                # CommentText
                f'"{comment["comment"].text}"',
                # CommentDate
                str(datetime.datetime.fromtimestamp(comment["comment"].as_dict['create_time'])),
                # CommentUpvotes
                str(comment["comment"].likes_count),
                # ParentCommentID (0 for top-level)
                "0"
            ]
        ))
        # Do the same for each reply in a comment chain
        for rid, reply in comment['replies'].items():
            comment_list.append(",".join(
                [
                    # CommentID
                    reply.id,
                    # UserName
                    reply.author.username,
                    # CommentText
                    f'"{reply.text}"',
                    # CommentDate
                    str(datetime.datetime.fromtimestamp(reply.as_dict['create_time'])),
                    # CommentUpvotes
                    str(reply.likes_count),
                    # ParentCommentID
                    cid
                ]
            ))

    # Write the comments to a CSV file
    with io.open(f"comments_{video_id}.csv", mode="w", encoding="utf-8") as f:
        f.write("CommentID,UserName,CommentText,CommentDate,CommentUpvotes,ParentCommentID\n")
        f.write("\n".join(comment_list))


if __name__ == "__main__":
    for video in VIDEOS:
        print(f"Processing video: {video}")
        store_comments_for_video(video)
