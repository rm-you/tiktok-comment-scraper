import asyncio
import datetime
import io

from TikTokApi import TikTokApi

TOKEN = "MS_TOKEN_HERE"
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
    '7326797908916096302',  # retry?

    '7424675691339189534',
    '7427522821766630699',  # retry?
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
]


async def get_replies(comment):
    replies = {}
    async for reply in comment.replies(count=10000):
        if reply.id in replies:
            continue
        replies[reply.id] = reply
    # print(f"Returning replies: {len(replies)}")
    return replies


async def get_comments(video, comments):
    async for comment in video.comments(count=10000):
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
        if len(comments) % 100 == 0:
            print(f"Comments: {len(comments)}")
    return comments


async def get_all_comments(video_id: int):
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[TOKEN], num_sessions=1, sleep_after=3, browser="firefox")
        video = api.video(id=video_id)
        comments = {}
        await get_comments(video, comments)

        return comments


def store_comments_for_video(video_id: int):
    all_comments = asyncio.run(get_all_comments(video_id))

    comment_list = []
    for cid, comment in all_comments.items():
        comment_list.append(",".join(
            [comment["comment"].id, comment["comment"].author.username, f'"{comment["comment"].text}"', str(datetime.datetime.fromtimestamp(comment["comment"].as_dict['create_time'])), str(comment["comment"].likes_count), "0"]
        ))
        for rid, reply in comment['replies'].items():
            comment_list.append(",".join(
                [reply.id, reply.author.username, f'"{reply.text}"', str(datetime.datetime.fromtimestamp(reply.as_dict['create_time'])), str(reply.likes_count), cid]
            ))

    with io.open(f"comments_{video_id}.csv", mode="w", encoding="utf-8") as f:
        f.write("CommentID,UserName,CommentText,CommentDate,CommentUpvotes,ParentCommentID\n")
        f.write("\n".join(comment_list))


if __name__ == "__main__":
    for video in VIDEOS:
        print(f"Processing video: {video}")
        store_comments_for_video(video)
