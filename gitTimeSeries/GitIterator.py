from git import Repo


class GitIterator(object):
    """
    Clase para atravesar un repositorio Git (iterador).
    Puede concentrarse en un único fichero o en el commit.
    La razón de crear esto y no el iterador que provee pygit (importado) es que dicho iterador solo funciona hacia el pasado
    """

    def __init__(self, repoPath, fpath=None, reverse=True, minDate=None):
        """
        Construye un iterador para atravesar un repositorio git, bien hacia el futuro, bien hacia el pasado
        :param repoPath: Ubicación del repositorio
        :param fpath: Ubicación del fichero (path absoluto dentro del repo). Opcional, si se indica devuelve un handle
                      para abrir el fichero en cuestión. Si no, devuelve la estructura commit. Ver fileFromCommit
        :param reverse: Opcional (def True). True = de ayer hacia hoy. False = De hoy hacia ayer
        :param minDate: Opcional. Solo carga los commit a partir de cierta fecha hacia el futuro
        """

        self.repoPath = repoPath
        self.repo = Repo(repoPath)
        self.fPath = fpath
        self.minDate = minDate
        self.reverse = reverse
        self.currCommit = 0

        self.commitList = [commitID for commitID in self.repo.iter_commits() if self.matchDate(commitID)]
        if reverse:
            self.commitList.reverse()

    def __iter__(self):
        return self

    def __next__(self):
        if self.currCommit >= len(self.commitList):
            raise StopIteration(f"Repository located at {self.repoPath} completely traversed")

        commit = self.commitList[self.currCommit]
        self.currCommit += 1

        if self.fPath is None:
            return commit
        else:
            return fileFromCommit(self.fPath, commit)

    def __str__(self):
        infoFichero = f" Interesado en {self.fPath}." if self.fPath else ""
        infoSentido = "adelante" if self.reverse else "atras"
        infoPendientes = "Exhausto." if self.currCommit >= len(
            self.commitList) else f"Quedan {len(self.commitList) - self.currCommit} entradas."
        result = f"Git Repo en {self.repoPath}.{infoFichero} Hacia {infoSentido} en el tiempo. Estado: {self.currCommit}. {infoPendientes}"

        return result

    def matchDate(self, commit):
        if self.minDate is None:
            return True
        else:
            return self.minDate >= commit.committed_datetime


def fileFromCommit(fPath, commit):
    commitTree = commit.tree

    for blob in commitTree.blobs:
        if blob.path == fPath:
            fileHandle = blob.data_stream
            return fileHandle
    else:  # File not in commit version of repo
        return None
