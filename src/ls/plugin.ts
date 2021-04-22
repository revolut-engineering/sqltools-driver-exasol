import { ILanguageServerPlugin } from '@sqltools/types';
import ExasolDriver from './driver';
import { DRIVER_ALIASES } from './../constants';

const ExasolDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server.getContext().drivers.set(value, ExasolDriver as any);
    });
  }
}

export default ExasolDriverPlugin;
