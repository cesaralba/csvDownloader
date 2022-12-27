from git import Repo

import pandas as pd


class GitIterator(object):
    """
    Class to traverse a git repository (iterator).

    Using this and not the iterator provided by pygit because pygit's iterator only work backwards
    """

    def __init__(self, repoPath, reverse=True, minDate=None, strictMinimum=True):
        """
        Builds an iterator to traverse a git repository. It traverses either FROM past (reverse=True) or TO
        past (reverse=False).
        :param repoPath: Location in disk of git repository
        :param reverse: Optional (def True). True = FROM past. False = TO past
        :param minDate: Optional. Just traverses commits from a certain date to future.
        :param strictMinimum: Optional (def True). True = Commit date is strictly bigger than minDate
                                                   False = Commit date is bigger or equal than minDate
        """

        self.repoPath = repoPath
        self.repo = Repo(repoPath)
        self.minDate = pd.to_datetime(minDate) if minDate else minDate
        self.reverse = reverse
        self.currCommit = 0

        self.commitList = [commitID for commitID in self.repo.iter_commits() if self.matchDate(commitID, strictMinimum)]

        if reverse:
            self.commitList.reverse()

    def __iter__(self):
        return self

    def __next__(self):
        if self.currCommit >= len(self.commitList):
            raise StopIteration(f"Repository located at {self.repoPath} completely traversed")

        commit = self.commitList[self.currCommit]
        self.currCommit += 1

        return commit

    def __str__(self):
        infoSentido = "Forwards" if self.reverse else "Backwards"
        infoPendientes = "Exhausted." if self.currCommit >= len(
            self.commitList) else f"{len(self.commitList) - self.currCommit} entries remaining"
        result = f"Git Repo in {self.repoPath}. {infoSentido} in time. State: {self.currCommit}. {infoPendientes}"

        return result

    def __len__(self):
        return len(self.commitList) - self.currCommit

    def matchDate(self, commit, strictMinimum=True):
        if self.minDate is None:
            return True
        else:
            return (self.minDate < commit.committed_datetime) if strictMinimum else (
                    self.minDate <= commit.committed_datetime)


def fileFromCommit(fPath, commit):
    commitTree = commit.tree

    for blob in commitTree.blobs:
        if blob.path == fPath:
            fileHandle = blob.data_stream
            return fileHandle


def saveTempFileCondition(filename=None, step=0):
    return (filename is not None) & (step > 0)
