import qualified Data.ByteString.Char8 as BC
import Data.Hadoop.Types
import Data.List.Split (splitOn)
import qualified Data.Text as T
import Network.Hadoop.Hdfs
import Network.Hadoop.Read
import System.Environment

main :: IO ()
main = do
  args <- getArgs
  case args of
   ("print": user: path: nodes) -> printHdfsFile (mkConfig user nodes) path
   ("copyToLocal": destFile: user: path: nodes) -> copyHdfsFileToLocal destFile (mkConfig user nodes) path
   _ -> error "usage: command [cmd-args] user path nodes\n commands:\n print\n copyToLocal destFile"
  where
    mkConfig u ns = HadoopConfig (T.pack u) (map toEndpoint ns) Nothing
      where
        toEndpoint :: String -> Endpoint
        toEndpoint str = let es = splitOn ":" str in if length es == 2 then Endpoint (T.pack $ es !! 0) (read $ es !! 1) else error $ "illegal host: "++str

printHdfsFile :: HadoopConfig -> String -> IO ()
printHdfsFile = withHdfsReader print

copyHdfsFileToLocal :: FilePath -> HadoopConfig -> String -> IO ()
copyHdfsFileToLocal destFile = withHdfsReader $ BC.writeFile destFile

withHdfsReader :: (BC.ByteString -> IO ()) -> HadoopConfig -> String -> IO ()
withHdfsReader action config path =  do
  readHandle_ <- runHdfs' config $ openRead $ BC.pack path
  case readHandle_ of
   (Just readHandle) -> hdfsMapM_ action readHandle
   Nothing -> error "no read handle"
