import {Box, Spinner} from '@dagster-io/ui';
import * as React from 'react';

import {RawLogContent} from '../RawLogContent';
import {AppContext} from '../app/AppContext';

import {useComputeLogs} from './useComputeLogs';

interface ComputeLogPanelProps {
  runId: string;
  ioType: string;
  setComputeLogUrl: (url: string | null) => void;
}

interface ComputeLogPanelMaybeKeyProps extends ComputeLogPanelProps {
  computeLogKey?: string;
  stepKeys: string[];
}

export const ComputeLogPanel: React.FC<ComputeLogPanelMaybeKeyProps> = (props) => {
  const {runId, stepKeys, computeLogKey, ioType, setComputeLogUrl} = props;

  if (!stepKeys.length || !computeLogKey) {
    return (
      <Box
        flex={{justifyContent: 'center', alignItems: 'center'}}
        style={{flex: 1, height: '100%'}}
      >
        <Spinner purpose="section" />
      </Box>
    );
  }

  return (
    <ComputeLogsPanelWithKey
      runId={runId}
      computeLogKey={computeLogKey}
      ioType={ioType}
      setComputeLogUrl={setComputeLogUrl}
    />
  );
};

interface ComputeLogPanelWithKeyProps extends ComputeLogPanelProps {
  computeLogKey: string;
}

const resolveDownloadUrl = (rootServerURI: string, downloadUrl: string | null) => {
  if (!downloadUrl) {
    return null;
  }
  const isRelativeUrl = (x?: string) => x && x.startsWith('/');
  return isRelativeUrl(downloadUrl) ? rootServerURI + downloadUrl : downloadUrl;
};

const ComputeLogsPanelWithKey: React.FC<ComputeLogPanelWithKeyProps> = React.memo((props) => {
  const {runId, computeLogKey, ioType, setComputeLogUrl} = props;
  const {rootServerURI} = React.useContext(AppContext);

  const {isLoading, stdout, stderr} = useComputeLogs(runId, computeLogKey);
  const logData = ioType === 'stdout' ? stdout : stderr;
  const downloadUrl = resolveDownloadUrl(rootServerURI, logData?.downloadUrl || null);

  return (
    <div style={{flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column'}}>
      <ContentWrapper
        content={stdout ? stdout.data : null}
        path={stdout ? stdout.path : undefined}
        downloadUrl={downloadUrl}
        isLoading={isLoading}
        isVisible={ioType === 'stdout'}
        setComputeLogUrl={setComputeLogUrl}
      />
      <ContentWrapper
        content={stderr ? stderr.data : null}
        path={stderr ? stderr.path : undefined}
        downloadUrl={downloadUrl}
        isLoading={isLoading}
        isVisible={ioType === 'stderr'}
        setComputeLogUrl={setComputeLogUrl}
      />
    </div>
  );
});

const ContentWrapper = ({
  isLoading,
  isVisible,
  content,
  path,
  downloadUrl,
  setComputeLogUrl,
}: {
  isVisible: boolean;
  isLoading: boolean;
  content: string | null;
  path?: string;
  downloadUrl: string | null;
  setComputeLogUrl: (url: string | null) => void;
}) => {
  React.useEffect(() => {
    setComputeLogUrl(downloadUrl);
  }, [setComputeLogUrl, downloadUrl]);
  return (
    <RawLogContent
      logData={content}
      isLoading={isLoading}
      isVisible={isVisible}
      downloadUrl={downloadUrl}
      location={path}
    />
  );
};
