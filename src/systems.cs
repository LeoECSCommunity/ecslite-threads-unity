// -------------------------------------------------------------------------------------
// The MIT License
// Unity Jobs support for LeoECS Lite https://github.com/Leopotam/ecslite-threads-unity
// Copyright (c) 2021 Leopotam <leopotam@gmail.com>
// -------------------------------------------------------------------------------------

using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;

namespace Leopotam.EcsLite.Threads.Unity {
    public abstract class EcsUnityJobSystem<TJob, T1> : EcsUnityJobSystemBase
        where TJob : struct, IEcsUnityJob<T1>
        where T1 : unmanaged {
        EcsFilter _filter;
        EcsPool<T1> _pool1;

        public override void Run (EcsSystems systems) {
            if (_filter == null) {
                var world = GetWorld (systems);
                _pool1 = world.GetPool<T1> ();
                _filter = GetFilter (world);
            }
            var nativeEntities = NativeHelpers.WrapToNative (_filter.GetRawEntities ());
            var nativePool1 = NativeHelpers.WrapToNative (_pool1.GetRawItems ());
            TJob job = default;
            job.Init (nativeEntities.Array, nativePool1.Array);
            SetData (systems, ref job);
            job.Schedule (_filter.GetEntitiesCount (), GetChunkSize (systems)).Complete ();
#if UNITY_EDITOR
            NativeHelpers.UnwrapFromNative (nativeEntities);
            NativeHelpers.UnwrapFromNative (nativePool1);
#endif
        }

        protected virtual void SetData (EcsSystems systems, ref TJob job) { }
    }

    public abstract class EcsUnityJobSystem<TJob, T1, T2> : EcsUnityJobSystemBase
        where TJob : struct, IEcsUnityJob<T1, T2>
        where T1 : unmanaged
        where T2 : unmanaged {
        EcsFilter _filter;
        EcsPool<T1> _pool1;
        EcsPool<T2> _pool2;

        public override void Run (EcsSystems systems) {
            if (_filter == null) {
                var world = GetWorld (systems);
                _pool1 = world.GetPool<T1> ();
                _pool2 = world.GetPool<T2> ();
                _filter = GetFilter (world);
            }
            var nativeEntities = NativeHelpers.WrapToNative (_filter.GetRawEntities ());
            var nativePool1 = NativeHelpers.WrapToNative (_pool1.GetRawItems ());
            var nativePool2 = NativeHelpers.WrapToNative (_pool2.GetRawItems ());
            TJob job = default;
            job.Init (nativeEntities.Array, nativePool1.Array, nativePool2.Array);
            SetData (systems, ref job);
            job.Schedule (_filter.GetEntitiesCount (), GetChunkSize (systems)).Complete ();
#if UNITY_EDITOR
            NativeHelpers.UnwrapFromNative (nativeEntities);
            NativeHelpers.UnwrapFromNative (nativePool1);
            NativeHelpers.UnwrapFromNative (nativePool2);
#endif
        }

        protected virtual void SetData (EcsSystems systems, ref TJob job) { }
    }

    public abstract class EcsUnityJobSystem<TJob, T1, T2, T3> : EcsUnityJobSystemBase
        where TJob : struct, IEcsUnityJob<T1, T2, T3>
        where T1 : unmanaged
        where T2 : unmanaged
        where T3 : unmanaged {
        EcsFilter _filter;
        EcsPool<T1> _pool1;
        EcsPool<T2> _pool2;
        EcsPool<T3> _pool3;

        public override void Run (EcsSystems systems) {
            if (_filter == null) {
                var world = GetWorld (systems);
                _pool1 = world.GetPool<T1> ();
                _pool2 = world.GetPool<T2> ();
                _pool3 = world.GetPool<T3> ();
                _filter = GetFilter (world);
            }
            var nEntities = NativeHelpers.WrapToNative (_filter.GetRawEntities ());
            var nPool1 = NativeHelpers.WrapToNative (_pool1.GetRawItems ());
            var nPool2 = NativeHelpers.WrapToNative (_pool2.GetRawItems ());
            var nPool3 = NativeHelpers.WrapToNative (_pool3.GetRawItems ());
            TJob job = default;
            job.Init (nEntities.Array, nPool1.Array, nPool2.Array, nPool3.Array);
            SetData (systems, ref job);
            job.Schedule (_filter.GetEntitiesCount (), GetChunkSize (systems)).Complete ();
#if UNITY_EDITOR
            NativeHelpers.UnwrapFromNative (nEntities);
            NativeHelpers.UnwrapFromNative (nPool1);
            NativeHelpers.UnwrapFromNative (nPool2);
            NativeHelpers.UnwrapFromNative (nPool3);
#endif
        }

        protected virtual void SetData (EcsSystems systems, ref TJob job) { }
    }

    public abstract class EcsUnityJobSystem<TJob, T1, T2, T3, T4> : EcsUnityJobSystemBase
        where TJob : struct, IEcsUnityJob<T1, T2, T3, T4>
        where T1 : unmanaged
        where T2 : unmanaged
        where T3 : unmanaged
        where T4 : unmanaged {
        EcsFilter _filter;
        EcsPool<T1> _pool1;
        EcsPool<T2> _pool2;
        EcsPool<T3> _pool3;
        EcsPool<T4> _pool4;

        public override void Run (EcsSystems systems) {
            if (_filter == null) {
                var world = GetWorld (systems);
                _pool1 = world.GetPool<T1> ();
                _pool2 = world.GetPool<T2> ();
                _pool3 = world.GetPool<T3> ();
                _pool4 = world.GetPool<T4> ();
                _filter = GetFilter (world);
            }
            var nEntities = NativeHelpers.WrapToNative (_filter.GetRawEntities ());
            var nPool1 = NativeHelpers.WrapToNative (_pool1.GetRawItems ());
            var nPool2 = NativeHelpers.WrapToNative (_pool2.GetRawItems ());
            var nPool3 = NativeHelpers.WrapToNative (_pool3.GetRawItems ());
            var nPool4 = NativeHelpers.WrapToNative (_pool4.GetRawItems ());
            TJob job = default;
            job.Init (nEntities.Array, nPool1.Array, nPool2.Array, nPool3.Array, nPool4.Array);
            SetData (systems, ref job);
            job.Schedule (_filter.GetEntitiesCount (), GetChunkSize (systems)).Complete ();
#if UNITY_EDITOR
            NativeHelpers.UnwrapFromNative (nEntities);
            NativeHelpers.UnwrapFromNative (nPool1);
            NativeHelpers.UnwrapFromNative (nPool2);
            NativeHelpers.UnwrapFromNative (nPool3);
            NativeHelpers.UnwrapFromNative (nPool4);
#endif
        }

        protected virtual void SetData (EcsSystems systems, ref TJob job) { }
    }

    public abstract class EcsUnityJobSystemBase : IEcsRunSystem {
        public abstract void Run (EcsSystems systems);
        protected abstract int GetChunkSize (EcsSystems systems);
        protected abstract EcsFilter GetFilter (EcsWorld world);
        protected abstract EcsWorld GetWorld (EcsSystems systems);
    }

    public interface IEcsUnityJob<T1> : IJobParallelFor
        where T1 : unmanaged {
        void Init (
            NativeArray<int> entities,
            NativeArray<T1> pool);
    }

    public interface IEcsUnityJob<T1, T2> : IJobParallelFor
        where T1 : unmanaged
        where T2 : unmanaged {
        void Init (
            NativeArray<int> entities,
            NativeArray<T1> pool1,
            NativeArray<T2> pool2);
    }

    public interface IEcsUnityJob<T1, T2, T3> : IJobParallelFor
        where T1 : unmanaged
        where T2 : unmanaged
        where T3 : unmanaged {
        void Init (
            NativeArray<int> entities,
            NativeArray<T1> pool1,
            NativeArray<T2> pool2,
            NativeArray<T3> pool3);
    }

    public interface IEcsUnityJob<T1, T2, T3, T4> : IJobParallelFor
        where T1 : unmanaged
        where T2 : unmanaged
        where T3 : unmanaged
        where T4 : unmanaged {
        void Init (
            NativeArray<int> entities,
            NativeArray<T1> pool1,
            NativeArray<T2> pool2,
            NativeArray<T3> pool3,
            NativeArray<T4> pool4);
    }

    static class NativeHelpers {
        public static unsafe NativeWrappedData<T> WrapToNative<T> (T[] managedData) where T : unmanaged {
            fixed (void* ptr = managedData) {
#if UNITY_EDITOR
                var nativeData = NativeArrayUnsafeUtility.ConvertExistingDataToNativeArray<T> (ptr, managedData.Length, Allocator.None);
                var sh = AtomicSafetyHandle.Create ();
                NativeArrayUnsafeUtility.SetAtomicSafetyHandle (ref nativeData, sh);
                return new NativeWrappedData<T> { Array = nativeData, SafetyHandle = sh };
#else
                return new NativeWrappedData<T0> { Array = NativeArrayUnsafeUtility.ConvertExistingDataToNativeArray<T1> (ptr, managedData.Length, Allocator.None) };
#endif
            }
        }

#if UNITY_EDITOR
        public static void UnwrapFromNative<T1> (NativeWrappedData<T1> sh) where T1 : unmanaged {
            AtomicSafetyHandle.CheckDeallocateAndThrow (sh.SafetyHandle);
            AtomicSafetyHandle.Release (sh.SafetyHandle);
        }
#endif
        public struct NativeWrappedData<TT> where TT : unmanaged {
            public NativeArray<TT> Array;
#if UNITY_EDITOR
            public AtomicSafetyHandle SafetyHandle;
#endif
        }
    }
}