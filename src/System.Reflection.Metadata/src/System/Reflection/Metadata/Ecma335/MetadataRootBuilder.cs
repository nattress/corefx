// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection.PortableExecutable;

namespace System.Reflection.Metadata.Ecma335
{
    /// <summary>
    /// Builder of a Metadata Root to be embedded in a Portable Executable image.
    /// </summary>
    /// <remarks>
    /// Metadata root constitutes of a metadata header followed by metadata streams (#~, #Strings, #US, #Guid and #Blob).
    /// </remarks>
    public sealed class MetadataRootBuilder
    {
        private const string DefaultMetadataVersionString = "v4.0.30319";

        // internal for testing
        internal static readonly ImmutableArray<int> EmptyRowCounts = ImmutableArray.Create(new int[MetadataTokens.TableCount]);

        private readonly MetadataBuilder _tablesAndHeaps;
        private readonly SerializedMetadata _serializedMetadata;

        private readonly PEReader _reader;
        private MetadataSizes _sizes;
        /// <summary>
        /// Metadata version string.
        /// </summary>
        public string MetadataVersion { get; }

        /// <summary>
        /// True to suppresses basic validation of metadata tables. 
        /// The validation verifies that entries in the tables were added in order required by the ECMA specification.
        /// It does not enforce all specification requirements on metadata tables.
        /// </summary>
        public bool SuppressValidation { get; }

        /// <summary>
        /// Creates a builder of a metadata root.
        /// </summary>
        /// <param name="tablesAndHeaps">
        /// Builder populated with metadata entities stored in tables and values stored in heaps.
        /// The entities and values will be enumerated when serializing the metadata root.
        /// </param>
        /// <param name="metadataVersion">
        /// The version string written to the metadata header. The default value is "v4.0.30319".
        /// </param>
        /// <param name="suppressValidation">
        /// True to suppresses basic validation of metadata tables during serialization.
        /// The validation verifies that entries in the tables were added in order required by the ECMA specification.
        /// It does not enforce all specification requirements on metadata tables.
        /// </param>
        /// <exception cref="ArgumentNullException"><paramref name="tablesAndHeaps"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="metadataVersion"/> is too long (the number of bytes when UTF8-encoded must be less than 255).</exception>
        public MetadataRootBuilder(MetadataBuilder tablesAndHeaps, string metadataVersion = null, bool suppressValidation = false)
        {
            if (tablesAndHeaps == null)
            {
                Throw.ArgumentNull(nameof(tablesAndHeaps));
            }

            Debug.Assert(BlobUtilities.GetUTF8ByteCount(DefaultMetadataVersionString) == DefaultMetadataVersionString.Length);
            int metadataVersionByteCount = metadataVersion != null ? BlobUtilities.GetUTF8ByteCount(metadataVersion) : DefaultMetadataVersionString.Length;

            if (metadataVersionByteCount > MetadataSizes.MaxMetadataVersionByteCount)
            {
                Throw.InvalidArgument(SR.MetadataVersionTooLong, nameof(metadataVersion));
            }

            _tablesAndHeaps = tablesAndHeaps;
            MetadataVersion = metadataVersion ?? DefaultMetadataVersionString;
            SuppressValidation = suppressValidation;
            _serializedMetadata = tablesAndHeaps.GetSerializedMetadata(EmptyRowCounts, metadataVersionByteCount, isStandaloneDebugMetadata: false);
            _sizes = _serializedMetadata.Sizes;
        }
        
        /// <summary>
        /// Creates a builder of a metadata root using an existing IL image as the metadata source to copy from
        /// </summary>
        public MetadataRootBuilder(PEReader reader)
        {
            _reader = reader;

            Debug.Assert(HeapIndex.UserString == 0);
            Debug.Assert((int)HeapIndex.String == 1);
            Debug.Assert((int)HeapIndex.Blob == 2);
            Debug.Assert((int)HeapIndex.Guid == 3);

            var heapSizes = ImmutableArray.Create(
                reader.GetMetadataReader().UserStringHeap.Block.Length,
                reader.GetMetadataReader().StringHeap.Block.Length,
                reader.GetMetadataReader().BlobHeap.Block.Length,
                reader.GetMetadataReader().GuidHeap.Block.Length);
            
            _sizes = new MetadataSizes(GetRowCounts(reader.GetMetadataReader()), EmptyRowCounts, heapSizes, BlobUtilities.GetUTF8ByteCount(reader.GetMetadataReader().MetadataVersion), false);
        }

        private ImmutableArray<int> GetRowCounts(MetadataReader reader)
        {
            var rowCounts = ImmutableArray.CreateBuilder<int>(MetadataTokens.TableCount);
            rowCounts.Count = MetadataTokens.TableCount;

            rowCounts[(int)TableIndex.Assembly] = reader.GetTableRowCount(TableIndex.Assembly);
            rowCounts[(int)TableIndex.AssemblyRef] = reader.GetTableRowCount(TableIndex.AssemblyRef);
            rowCounts[(int)TableIndex.ClassLayout] = reader.GetTableRowCount(TableIndex.ClassLayout);
            rowCounts[(int)TableIndex.Constant] = reader.GetTableRowCount(TableIndex.Constant);
            rowCounts[(int)TableIndex.CustomAttribute] = reader.GetTableRowCount(TableIndex.CustomAttribute);
            rowCounts[(int)TableIndex.DeclSecurity] = reader.GetTableRowCount(TableIndex.DeclSecurity);
            rowCounts[(int)TableIndex.EncLog] = reader.GetTableRowCount(TableIndex.EncLog);
            rowCounts[(int)TableIndex.EncMap] = reader.GetTableRowCount(TableIndex.EncMap);
            rowCounts[(int)TableIndex.EventMap] = reader.GetTableRowCount(TableIndex.EventMap);
            rowCounts[(int)TableIndex.Event] = reader.GetTableRowCount(TableIndex.Event);
            rowCounts[(int)TableIndex.ExportedType] = reader.GetTableRowCount(TableIndex.ExportedType);
            rowCounts[(int)TableIndex.FieldLayout] = reader.GetTableRowCount(TableIndex.FieldLayout);
            rowCounts[(int)TableIndex.FieldMarshal] = reader.GetTableRowCount(TableIndex.FieldMarshal);
            rowCounts[(int)TableIndex.FieldRva] = reader.GetTableRowCount(TableIndex.FieldRva);
            rowCounts[(int)TableIndex.Field] = reader.GetTableRowCount(TableIndex.Field);
            rowCounts[(int)TableIndex.File] = reader.GetTableRowCount(TableIndex.File);
            rowCounts[(int)TableIndex.GenericParamConstraint] = reader.GetTableRowCount(TableIndex.GenericParamConstraint);
            rowCounts[(int)TableIndex.GenericParam] = reader.GetTableRowCount(TableIndex.GenericParam);
            rowCounts[(int)TableIndex.ImplMap] = reader.GetTableRowCount(TableIndex.ImplMap);
            rowCounts[(int)TableIndex.InterfaceImpl] = reader.GetTableRowCount(TableIndex.InterfaceImpl);
            rowCounts[(int)TableIndex.ManifestResource] = reader.GetTableRowCount(TableIndex.ManifestResource);
            rowCounts[(int)TableIndex.MemberRef] = reader.GetTableRowCount(TableIndex.MemberRef);
            rowCounts[(int)TableIndex.MethodImpl] = reader.GetTableRowCount(TableIndex.MethodImpl);
            rowCounts[(int)TableIndex.MethodSemantics] = reader.GetTableRowCount(TableIndex.MethodSemantics);
            rowCounts[(int)TableIndex.MethodSpec] = reader.GetTableRowCount(TableIndex.MethodSpec);
            rowCounts[(int)TableIndex.MethodDef] = reader.GetTableRowCount(TableIndex.MethodDef);
            rowCounts[(int)TableIndex.ModuleRef] = reader.GetTableRowCount(TableIndex.ModuleRef);
            rowCounts[(int)TableIndex.Module] = reader.GetTableRowCount(TableIndex.Module);
            rowCounts[(int)TableIndex.NestedClass] = reader.GetTableRowCount(TableIndex.NestedClass);
            rowCounts[(int)TableIndex.Param] = reader.GetTableRowCount(TableIndex.Param);
            rowCounts[(int)TableIndex.PropertyMap] = reader.GetTableRowCount(TableIndex.PropertyMap);
            rowCounts[(int)TableIndex.Property] = reader.GetTableRowCount(TableIndex.Property);
            rowCounts[(int)TableIndex.StandAloneSig] = reader.GetTableRowCount(TableIndex.StandAloneSig);
            rowCounts[(int)TableIndex.TypeDef] = reader.GetTableRowCount(TableIndex.TypeDef);
            rowCounts[(int)TableIndex.TypeRef] = reader.GetTableRowCount(TableIndex.TypeRef);
            rowCounts[(int)TableIndex.TypeSpec] = reader.GetTableRowCount(TableIndex.TypeSpec);

            rowCounts[(int)TableIndex.Document] = reader.GetTableRowCount(TableIndex.Document);
            rowCounts[(int)TableIndex.MethodDebugInformation] = reader.GetTableRowCount(TableIndex.MethodDebugInformation);
            rowCounts[(int)TableIndex.LocalScope] = reader.GetTableRowCount(TableIndex.LocalScope);
            rowCounts[(int)TableIndex.LocalVariable] = reader.GetTableRowCount(TableIndex.LocalVariable);
            rowCounts[(int)TableIndex.LocalConstant] = reader.GetTableRowCount(TableIndex.LocalConstant);
            rowCounts[(int)TableIndex.StateMachineMethod] = reader.GetTableRowCount(TableIndex.StateMachineMethod);
            rowCounts[(int)TableIndex.ImportScope] = reader.GetTableRowCount(TableIndex.ImportScope);
            rowCounts[(int)TableIndex.CustomDebugInformation] = reader.GetTableRowCount(TableIndex.CustomDebugInformation);

            return rowCounts.MoveToImmutable();
        }

        /// <summary>
        /// Returns sizes of various metadata structures.
        /// </summary>
        public MetadataSizes Sizes => _sizes;

        /// <summary>
        /// Serializes metadata root content into the given <see cref="BlobBuilder"/>.
        /// </summary>
        /// <param name="builder">Builder to write to.</param>
        /// <param name="methodBodyStreamRva">
        /// The relative virtual address of the start of the method body stream.
        /// Used to calculate the final value of RVA fields of MethodDef table.
        /// </param>
        /// <param name="mappedFieldDataStreamRva">
        /// The relative virtual address of the start of the field init data stream.
        /// Used to calculate the final value of RVA fields of FieldRVA table.
        /// </param>
        /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="methodBodyStreamRva"/> or <paramref name="mappedFieldDataStreamRva"/> is negative.</exception>
        /// <exception cref="InvalidOperationException">
        /// A metadata table is not ordered as required by the specification and <see cref="SuppressValidation"/> is false.
        /// </exception>
        public void Serialize(BlobBuilder builder, int methodBodyStreamRva, int mappedFieldDataStreamRva)
        {
            if (builder == null)
            {
                Throw.ArgumentNull(nameof(builder));
            }

            if (methodBodyStreamRva < 0)
            {
                Throw.ArgumentOutOfRange(nameof(methodBodyStreamRva));
            }

            if (mappedFieldDataStreamRva < 0)
            {
                Throw.ArgumentOutOfRange(nameof(mappedFieldDataStreamRva));
            }

            if (_reader != null)
            {
                var metadataByteReader = _reader.GetMetadata().GetReader();
                builder.WriteBytes(metadataByteReader.ReadBytes(_reader.GetMetadata().Length));
            }
            else
            {
                if (!SuppressValidation)
                {
                    _tablesAndHeaps.ValidateOrder();
                }

                // header:
                MetadataBuilder.SerializeMetadataHeader(builder, MetadataVersion, _serializedMetadata.Sizes);

                // #~ or #- stream:
                _tablesAndHeaps.SerializeMetadataTables(builder, _serializedMetadata.Sizes, _serializedMetadata.StringMap, methodBodyStreamRva, mappedFieldDataStreamRva);

                // #Strings, #US, #Guid and #Blob streams:
                _tablesAndHeaps.WriteHeapsTo(builder, _serializedMetadata.StringHeap);
            }
        }
    }
}
